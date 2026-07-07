# 02 — Delay Predictor (Java + Kafka Streams)

> Study notes for `delay-predictor/`. Written for someone with Java experience but
> little Kafka Streams or distributed stream-processing background. Code quotes are
> from the actual source. The delay math here is deliberately simplified — that's
> acknowledged head-on in §2 and §4 rather than dressed up.

---

## 1. Purpose

The delay predictor turns the raw firehose of vehicle positions into a per-route
"how late is this route running right now" signal: every 5 minutes, for every route
with activity, it estimates the actual headway (time between vehicles) from how many
distinct vehicles appeared, compares that against the scheduled headway loaded from
static GTFS into TimescaleDB, and publishes the difference as a `delay_score` to the
`delay-predictions` topic. It exists because raw positions answer "where is bus 8351?"
but the dashboard needs "is route 504 in trouble?" — an aggregation over time, which is
exactly the job stream processing frameworks are built for.

---

## 2. Line-by-line walkthrough

Five source files matter: `DelayPredictorApp.java` (the topology),
`TimescaleDBClient.java` (JDBC lookups), `StringSetSerde.java` (state serialization),
`AppConfig.java` (env-var config), and `application.properties` — plus `pom.xml` and the
Dockerfile, which each contain a decision that once broke production.

### Block 1: what kind of application is this? (no Spring, no framework)

```java
public class DelayPredictorApp {

    private static final Logger log = LoggerFactory.getLogger(DelayPredictorApp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static final Duration WINDOW_GRACE = Duration.ofSeconds(30);
    private static final ZoneId TORONTO = ZoneId.of("America/Toronto");

    public static void main(String[] args) {
```

**What it does.** A plain `main()` class — no `@SpringBootApplication`, no embedded web
server, no dependency injection container. The process consists of exactly one thing: a
Kafka Streams runtime executing a topology.

**Why no Spring Boot (a real decision, see §4.4).** Kafka Streams is *itself* the
application framework here: it manages its own threads, its own consumer/producer, its
own local state. What Spring Boot would add — a servlet container, component scanning,
autoconfiguration — is dead weight for a process with no HTTP endpoints and three
dependencies to wire. Contrast with this repo's `api-gateway`, which *does* use Spring
Boot, because it genuinely needs a web server and WebSocket support. Framework choice
followed need, per service.

**Kafka Streams concept #1 — what Kafka Streams even is.** A library (not a server!)
that turns a JVM process into a stream processor. You declare a *topology* — a graph of
operations like filter/group/window/aggregate — and the library runs it: it consumes
from input topics, maintains any state the operations need in embedded local stores,
and produces to output topics. Everything runs inside your process; there is no cluster
to submit jobs to (unlike Spark/Flink). Scaling out = starting another copy of the same
app with the same `application.id`; the instances split the input partitions between
them automatically, using Kafka's ordinary consumer-group mechanics.

**The three constants are the service's soul.** `WINDOW_SIZE`/`WINDOW_GRACE` define the
time-bucketing (Block 5), and `TORONTO` exists because the *schedule* is in local time —
computing "hour of day" in UTC would compare 8 AM rush-hour traffic against the 3 AM
schedule (Toronto is UTC-4/-5). Time zone bugs in scheduled-vs-actual comparisons are
exactly the kind of thing that produces plausible-looking nonsense.

### Block 2: configuration without Spring — `application.properties` + `AppConfig`

```properties
kafka.bootstrap.servers=${KAFKA_BROKER:kafka:29092}
kafka.schema.registry.url=${SCHEMA_REGISTRY_URL:http://schema-registry:8081}
kafka.application.id=delay-predictor
kafka.input.topic=vehicle-positions
kafka.output.topic=delay-predictions
```

```java
// AppConfig.java (core of it)
private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^:}]+)(?::([^}]*))?}");
...
static String resolve(String raw) {
    Matcher matcher = PLACEHOLDER.matcher(raw);
    StringBuilder out = new StringBuilder();
    while (matcher.find()) {
        String env = System.getenv(matcher.group(1));
        String fallback = matcher.group(2) == null ? "" : matcher.group(2);
        matcher.appendReplacement(out, Matcher.quoteReplacement(env != null ? env : fallback));
    }
    matcher.appendTail(out);
    return out.toString();
}
```

**What it does.** The properties file uses Spring's familiar `${ENV_VAR:default}`
placeholder syntax — but there's no Spring here, so `AppConfig` implements the
resolution with a ~15-line regex: for each `${NAME:default}`, substitute the env var if
set, else the default.

**Why.** Twelve-factor config (same image, env-driven settings) without dragging in a
framework for one feature. The honest assessment: this is a small reimplementation of
something Spring gives you for free, and it supports less (no nested placeholders, no
profiles). It was the right size for the need, but "I wrote a tiny version of a thing
frameworks do" is worth being able to defend — the defense is that the alternative was a
50 MB dependency for a string-substitution feature.

**Interview-worthy detail:** `Matcher.quoteReplacement` — without it, a `$` in an env
var's *value* would be interpreted as a regex group reference and corrupt the config.
Escaping data that flows into pattern-position is the same class of thinking as SQL
parameterization.

### Block 3: bootstrapping the Streams runtime

```java
Properties streamsProps = new Properties();
streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
// The topic may contain non-Avro records (e.g. from earlier JSON
// producers); skip them instead of crashing the application.
streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);

KafkaStreams streams = new KafkaStreams(
        buildTopology(inputTopic, outputTopic, schemaRegistryUrl, db), streamsProps);

CountDownLatch latch = new CountDownLatch(1);
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    streams.close(Duration.ofSeconds(10));
    db.close();
    latch.countDown();
}));

streams.start();
latch.await();
```

**`APPLICATION_ID_CONFIG` is the most consequential line.** It becomes: the consumer
group id (so multiple instances share partitions), the prefix of every *internal topic*
the app creates (state changelogs, repartition topics — you can literally see
`delay-predictor-KSTREAM-AGGREGATE-STATE-STORE-0000000003-...` topics on the broker),
and the namespace of the local state directory on disk. Change it and the app becomes a
stranger to its own history: it re-consumes from scratch and abandons its old state.

**The deserialization exception handler is a scar with a comment.** This service was
built *after* the ingestion service migrated from JSON to Avro, and the old JSON bytes
were still sitting in `vehicle-positions` (Kafka doesn't delete data on format changes
— retention does). Streams defaults to consuming from the earliest offset, so on first
start it hit non-Avro bytes and the default handler policy would kill the app.
`LogAndContinueExceptionHandler` says "log the bad record, skip it, keep going." The
tradeoff is real: this also silently skips *genuinely* corrupted future data. For a
telemetry pipeline where one lost position is meaningless, continue is right; for a
payments pipeline you'd choose the opposite (fail loudly, quarantine to a DLQ).

**The shutdown dance.** `streams.start()` spins up background processing threads and
returns immediately; `latch.await()` parks `main` forever (same problem as Go's
`wg.Wait()` in the ingestion doc: keep the JVM alive). The JVM shutdown hook gives
Streams up to 10 seconds to close cleanly on `docker stop` — flushing state, committing
offsets — which is what makes restarts resume mid-stream instead of reprocessing big
ranges. Rough edge worth admitting: if close exceeds 10s, Docker's own kill timer
(default 10s too) may SIGKILL mid-flush anyway; at-least-once semantics absorb that
(§5, exactly-once entry).

### Block 4: the serde — bridging Avro/Schema Registry into Streams

```java
@SuppressWarnings("unchecked")
private static Serde<VehiclePosition> vehiclePositionSerde(String schemaRegistryUrl) {
    Map<String, Object> serdeConfig = Map.of(
            "schema.registry.url", schemaRegistryUrl,
            "specific.avro.reader", true);

    KafkaAvroSerializer serializer = new KafkaAvroSerializer();
    serializer.configure(serdeConfig, false);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(serdeConfig, false);

    return (Serde<VehiclePosition>) (Serde<?>) Serdes.serdeFrom(serializer, deserializer);
}
```

**Kafka Streams concept #2 — serdes.** Kafka stores bytes; Streams operates on objects.
A *serde* (serializer + deserializer) is the two-way converter, and Streams needs one
for every point where data crosses a byte boundary: reading input, writing output, and
— less obviously — writing to state stores and their backing topics. Forgetting that
third case is the classic Streams beginner error ("why do I need a serde for my
aggregate type?" — because the aggregate lives in a store that persists bytes).

**What this one does.** Wraps Confluent's Avro serializer/deserializer as a serde. The
deserializer reads the 5-byte Confluent wire prefix the Go producer wrote (magic byte +
schema id), fetches that schema from the registry (cached after first use), and — because
of `specific.avro.reader=true` — decodes into the *generated* `VehiclePosition` class
rather than a generic field-map. That gives typed accessors (`vp.getRouteId()`) and
compile-time safety against schema drift.

**Where does `ca.ttc.intelligence.VehiclePosition` come from? Nobody wrote it.** The
`avro-maven-plugin` (pom, Block 8) generates it at build time from the *shared*
`schemas/vehicle_position.avsc` — the same file the Go producer loads at runtime. One
schema file, two languages, no hand-kept copies. When the schema gained three new
fields, rebuilding this service regenerated the class with the new getters; no manual
edits.

**The ugly double-cast** `(Serde<VehiclePosition>) (Serde<?>)` exists because Confluent's
classes are typed `Serializer<Object>`, and Java generics won't allow a direct cast
between incompatible parameterizations. Casting through the wildcard is the standard
workaround, with `@SuppressWarnings` as the admission fee. Safe here because
`specific.avro.reader=true` guarantees what actually comes out.

### Block 5: the topology — the heart of the service

```java
builder.stream(inputTopic, Consumed.with(Serdes.String(), vehicleSerde))
        .filter((key, vp) -> vp != null && vp.getRouteId() != null && !vp.getRouteId().isEmpty())
        .groupBy((key, vp) -> vp.getRouteId(), Grouped.with(Serdes.String(), vehicleSerde))
        .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE))
        .aggregate(
                HashSet::new,
                (route, vp, vehicles) -> {
                    vehicles.add(vp.getVehicleId());
                    return vehicles;
                },
                Materialized.with(Serdes.String(), new StringSetSerde()))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((windowedRoute, vehicles) -> KeyValue.pair(
                windowedRoute.key(), toPrediction(windowedRoute, vehicles, db)))
        .filter((route, prediction) -> prediction != null)
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
```

This is nine lines that would be several hundred lines of hand-written consumer code.
Step by step:

**`.stream(...)` — Kafka Streams concept #3, KStream vs KTable.** A `KStream` treats the
topic as an infinite sequence of independent *events* — every record matters. A `KTable`
treats it as a *changelog* — only the latest record per key matters (the ripple detector
uses that for alerts). Vehicle positions are events: each one is a fact ("vehicle X was
here at time T"), and we're counting facts, so KStream.

**`.filter(...)` — guarding against real-world dirt.** `vp != null` handles the
tombstones/skips from the deserialization handler; empty `route_id` is *common in the
actual feed* (out-of-service vehicles, deadheading buses report positions without route
assignments). Without this filter they'd form a garbage `""` route group.

**`.groupBy(...)` — and the invisible repartition topic.** The incoming records are
keyed by `vehicle_id` (the producer chose that for per-vehicle ordering). To aggregate
per *route*, records must be regrouped — and here's the distributed-systems moment: all
records for route 504 might live in different partitions, potentially processed by
different app instances. Streams solves this by writing everything back out to an
internal *repartition topic* keyed by route, and reading it back so each route's records
land in one place. You can see the evidence: a
`delay-predictor-...-repartition-value` subject actually appeared in our Schema
Registry, because the Avro serde auto-registered a schema for that internal topic.
**Cost worth knowing:** `groupBy` with a new key = a full extra write+read of the
stream through the broker. Unavoidable when you genuinely change the grouping key, but
it's why you never re-key casually.

**`.windowedBy(TimeWindows.ofSizeAndGrace(5min, 30s))` — concept #4, tumbling windows.**
This chops each route's stream into consecutive, non-overlapping 5-minute buckets
aligned to the epoch clock (…12:00–12:05, 12:05–12:10…). *Tumbling* = size equals
advance, every event belongs to exactly one window. The alternatives and why not:

- *Hopping* windows (e.g. 5-min window advancing every 1 min) would give smoother,
  overlapping readings but 5× the output and state, and each event counted in 5 windows.
- *Sliding* windows recompute per event pair — for continuous joins, wrong shape here.
- *Session* windows group by activity gaps — meaningful for user behavior, not for a
  fleet that never stops.

Tumbling is the "give me one number per route per 5 minutes" shape, which is exactly
what the dashboard's delay panel consumes.

The **epoch alignment** has a visible consequence we actually observed: windows close at
:00/:05/:10 of the hour regardless of when the app started, so after one restart the
first results appeared within ~1 minute (the app started at :04 and the :00–:05 window
closed at :05:30). Windows belong to the *data's* timeline, not the app's.

**Grace period — concept #5.** `ofSizeAndGrace(5min, 30s)` keeps a window open for
late-arriving records for 30 extra seconds past its end. "Late" is judged by *stream
time* — the max event timestamp seen so far — not the wall clock. Why 30s: our records'
timestamps are set at produce time and the producer polls every 15s, so genuine lateness
is bounded and small; a longer grace (Streams' historical default was 24h!) would delay
every result by that much when combined with suppression. This is the classic
**latency vs completeness** dial: shorter grace = fresher results, more risk of
dropping stragglers; longer = the reverse. 30s says "TTC data is near-real-time;
freshness wins."

**`.aggregate(...)` — concept #6, stateful processing and state stores.** For each
(route, window), maintain a `HashSet<String>` of vehicle ids: start with
`HashSet::new`, add each record's vehicle id. A set, not a counter, because the same
vehicle reports ~20 times per window (15s polls) and counting *positions* would inflate
"vehicles seen" 20×; distinctness is the point.

Where does that set *live*? In a **state store** — an embedded RocksDB database on the
container's local disk, one shard per partition. And because local disk dies with the
container, every store write is also appended to a **changelog topic** in Kafka
(`...-changelog`); on restart or instance failure, Streams replays the changelog to
rebuild the store. This is the framework's central trick: local-speed reads/writes with
broker-backed durability, and it's why the Dockerfile needs `libstdc++` (§4.3 — RocksDB
is C++ loaded over JNI).

`Materialized.with(Serdes.String(), new StringSetSerde())` supplies the serdes for that
store. `StringSetSerde` is ~40 lines wrapping Jackson: a `HashSet<String>` ⇄ JSON bytes.
Honest tradeoff: JSON is a lazy, inefficient encoding for a state store (repeated
quoting, no schema), chosen because the sets are small (≤ dozens of ids) and writing a
compact custom binary format would be optimizing the wrong thing.

**`.suppress(untilWindowCloses(unbounded()))` — concept #7, and easily the most
misunderstood operator.** Without suppression, a windowed aggregate emits a *new result
on every update* — route 504's 12:00–12:05 window would emit ~40 intermediate results
("1 vehicle… 2 vehicles… 3…") as records arrive. Downstream would see a stream of
half-baked delay scores. `untilWindowCloses` holds results back and emits **exactly one
final result per window**, after window end + grace.

Two sharp edges every interviewer loves:
1. *Emission needs stream time to advance.* A window "closes" only when a newer record
   moves stream time past end+grace. If the input stops entirely, the last window's
   result is never emitted — it sits in the suppression buffer. Fine here (TTC data
   flows 24/7), but a low-traffic topic with suppression can look "stuck" and it's by
   design.
2. `BufferConfig.unbounded()` lets the suppression buffer grow without limit. Bounded
   configs can *shut down the app* or emit early when full. Unbounded is safe here
   because per-window state is tiny and windows expire quickly; on a high-cardinality
   keyspace it would be a memory time bomb.

**`.toStream().map(...)` — from table-land back to event-land, and the join-ish part.**
The windowed aggregate is a `KTable<Windowed<String>, Set<String>>`; `toStream()`
converts the finalized results into events. The key type `Windowed<String>` carries both
the route and the window bounds — `windowedRoute.key()` and `.window().start()/.end()`.
The `map` calls `toPrediction`, which is where the DB lookup happens (next block), and
the trailing `.filter(prediction != null)` drops routes we couldn't score.

**`.to(outputTopic, Produced.with(String, String))` — JSON out.** The output is a plain
JSON string, *not* Avro. That looks inconsistent but isn't — see §3.

### Block 6: the scoring function — where honesty matters

```java
int vehicleCount = vehicles.size();
double actualHeadway = WINDOW_SIZE.toMinutes() / (double) vehicleCount;

int hourOfDay = ZonedDateTime.ofInstant(Instant.ofEpochMilli(windowStart), TORONTO).getHour();
OptionalDouble scheduled = db.getScheduledHeadway(routeId, hourOfDay);
if (scheduled.isEmpty()) {
    log.debug("no scheduled headway for route={} hour={}; skipping window", routeId, hourOfDay);
    return null;
}

double delayScore = actualHeadway - scheduled.getAsDouble();
```

**The math, and what's wrong with it (know this cold).** `actual_headway = 5 minutes /
distinct vehicles in window` says: if 5 vehicles appeared on the route in 5 minutes,
one arrives "every minute." That's a *fleet-density proxy for headway*, not real
headway. Real headway is the gap between consecutive arrivals *at a fixed stop, in one
direction*. This version:

- mixes both directions (a route with 3 eastbound + 3 westbound vehicles looks twice as
  frequent as it feels to a rider),
- measures route-wide presence, not spacing (5 bunched vehicles score the same as 5
  perfectly spaced ones — and bunching is precisely the delay signature riders hate),
- is bounded below by the window size divided by fleet size.

Why ship it anyway: it's *computable from exactly the data in one window with no extra
state*, it's monotonically sensible (fewer vehicles ⇒ higher score ⇒ "worse"), and the
matching `scheduled_headway` from the loader was computed with a *symmetrical*
simplification (trips per hour, busier direction), so the comparison is apples-to-apples
crude rather than mixed. The upgrade path is real and known: keying by
route+direction (the crowding estimator already does this) and measuring
per-stop arrival gaps using `current_stop_sequence`. A senior interviewer will respect
"here's the simplification, here's why it was symmetric, here's the upgrade path" far
more than pretending this is transit science.

**`scheduled.isEmpty() → return null` — the accepted gap (§4.6).** No schedule row
means no comparison is possible; emitting a score of `actual - 0` or a null-filled JSON
would be *worse* than silence because downstream can't distinguish "very delayed" from
"unknown route." The record is dropped and (rough edge, honestly) logged only at
`debug`, which is invisible at default log level — the *crowding estimator*, built two
sessions later, logs the same situation at WARN because we'd learned by then that silent
drops cost debugging hours. This file just never got retrofitted.

**`hourOfDay` in Toronto time** — already covered, but note it's derived from
`windowStart` (event time), not `System.currentTimeMillis()` (processing time). If the
app replays yesterday's data (e.g. after being down), each window is compared against
the schedule for *the hour the data describes*, not the hour the code happens to run.
Small line, correct-by-construction replay semantics.

### Block 7: `TimescaleDBClient` — plain JDBC with a tiny pool

```java
private static final String HEADWAY_QUERY =
        "SELECT scheduled_headway_minutes FROM scheduled_headways WHERE route_id = ? AND hour_of_day = ?";
...
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:postgresql://" + host + ":" + port + "/" + name);
...
config.setMaximumPoolSize(2);
...
public OptionalDouble getScheduledHeadway(String routeId, int hourOfDay) {
    try (Connection conn = dataSource.getConnection();
         PreparedStatement stmt = conn.prepareStatement(HEADWAY_QUERY)) {
        ...
    } catch (SQLException e) {
        log.warn("scheduled headway lookup failed for route={} hour={}: {}", ...);
        return OptionalDouble.empty();
    }
}
```

**What it does.** One parameterized query against the `scheduled_headways` table that
`gtfs-loader` populates, through a HikariCP pool of **two** connections. No JPA, no
Spring Data — for one query, an ORM is pure ceremony.

**Why a pool at all for one query?** Because creating a Postgres connection costs a TCP
handshake + auth round trips (tens of ms), and this lookup runs on the hot path of
window emission. Pool size 2 (not Hikari's default 10) because the math says so:
lookups happen once per route per 5 minutes ≈ 170 queries / 300s — one connection is
nearly idle, two is headroom. Right-sizing pools to arithmetic instead of defaults is a
cheap way to look thoughtful in a systems interview.

**Failure = `OptionalDouble.empty()`, not an exception.** A DB blip shouldn't crash the
stream — the window's result is skipped (with a WARN this time) and the next window
retries naturally. `OptionalDouble` instead of returning `-1` or `null` forces every
caller to confront absence explicitly; the compiler won't let you accidentally do
arithmetic on a missing value.

**A subtlety worth being able to discuss: calling a database from inside a topology at
all.** Purist Kafka Streams design says side-effectful lookups don't belong in
operators — the "right" way is to stream the schedule *into* a `GlobalKTable` and join
against it, making the lookup local, deterministic, and replayable. We call JDBC anyway
because: the table is effectively static (reloaded quarterly), the call happens only at
window close (~dozens of queries per 5 minutes, not per record — suppression made this
viable), and a GlobalKTable would require a schedule→Kafka feed that doesn't exist. The
real costs accepted: a DB outage degrades output (windows skipped), and results are no
longer purely a function of the input stream. Defensible, but know it's the pragmatic
choice, not the canonical one.

### Block 8: `pom.xml` — four dependencies with war stories

```xml
<!-- jackson-databind comes in transitively via avro/kafka-avro-serializer;
     pinning a newer version here mixes jackson-core/databind versions and
     fails at runtime with NoSuchFieldError. -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>2.0.12</version>
</dependency>
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>2.0.12</version>
</dependency>
```

That comment marks a crash site (§4.2), and the *pair* of SLF4J artifacts marks another
(§4.1). Other notable pieces:

- **The Confluent Maven repository** (`packages.confluent.io/maven`) — Confluent's
  serializers aren't on Maven Central; forgetting this repo block yields an unresolvable
  `kafka-avro-serializer` and confusion.
- **`avro-maven-plugin`** bound to `generate-sources`, reading
  `${project.basedir}/../schemas` — the *shared* schema directory at the repo root —
  and generating Java classes into `target/generated-sources/avro`.
  `<stringType>String</stringType>` matters more than it looks: without it Avro
  generates fields as `CharSequence` (backed by Avro's `Utf8`), and
  `utf8Instance.equals("504")` is `false` — a legendary source of "my join never
  matches" bugs. One line makes all generated strings real `java.lang.String`.
- **`maven-assembly-plugin`** builds a fat jar (`jar-with-dependencies`) with a
  `Main-Class` manifest, so the Docker runtime stage is exactly `java -jar app.jar` —
  no classpath assembly, no Maven at runtime. (Spring Boot apps get this via their own
  repackaging plugin; plain Java needs assembly or shade.)

### Block 9: the Dockerfile — two stages and one hard-won `apk add`

```dockerfile
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build/delay-predictor
COPY delay-predictor/pom.xml .
RUN mvn -q -B dependency:go-offline
COPY schemas /build/schemas
COPY delay-predictor/src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:17-jre-alpine
# Kafka Streams state stores use RocksDB via JNI, which needs libstdc++
RUN apk add --no-cache libstdc++
WORKDIR /app
COPY --from=build /build/delay-predictor/target/delay-predictor-1.0.0-SNAPSHOT-jar-with-dependencies.jar app.jar
CMD ["java", "-jar", "app.jar"]
```

Same multi-stage + dependency-layer-caching pattern as the Go service (build image
~1 GB with Maven, runtime image ~200 MB JRE-only; `pom.xml` copied and
`dependency:go-offline` run *before* sources so code edits don't re-download the
world). Two specifics:

- The build context is the **repo root** (compose sets `context: .`), because the
  build needs `schemas/` which lives outside this service's directory — the pom's
  `../schemas` resolves inside the image because the Dockerfile recreates that relative
  layout (`/build/schemas` next to `/build/delay-predictor`).
- `apk add libstdc++` is one line that cost a debugging session (§4.3). Alpine's
  minimalism is a feature until a JNI library needs the C++ runtime that glibc-based
  images carry by default.

---

## 3. How this service connects to the rest of the system

**Consumes:** `vehicle-positions` (Avro, keyed by `vehicle_id`, 3 partitions) — the Go
ingestion service's output. Plus, at startup, schemas *from* Schema Registry (via the
deserializer) and generated classes from the shared `schemas/` directory at build time.

**Reads from TimescaleDB:** `scheduled_headways(route_id, hour_of_day,
scheduled_headway_minutes)` — the baseline to compare against, computed by
`gtfs-loader` from the static GTFS schedule (weekday service, busier direction — a
matching simplification to this service's own). This is the only reason the service
touches a database at all; everything else is Kafka-to-Kafka.

**Produces:** `delay-predictions` — one JSON message per route per closed 5-minute
window, keyed by route:

```json
{"route_id":"504","window_start":1719530400000,"window_end":1719530700000,
 "actual_headway_minutes":8.5,"scheduled_headway_minutes":5.0,
 "delay_score":3.5,"computed_at":1719530712000}
```

**Why Avro in but JSON out — and why that's not inconsistent.** The format choice
follows the *contract's blast radius*. `vehicle-positions` is the platform's core
artery: multi-language (Go writes, Java reads), high-volume (~5M records/day), consumed
by three services, and it *evolves* (it's on schema v2 already) — everything Schema
Registry is for. `delay-predictions` is a leaf: one producer, one consumer (the API
gateway, which parses it once and forwards the same JSON to browsers over WebSocket),
tiny volume (~2K records/day), and its natural final destination is JavaScript. Making
it Avro would buy contract enforcement nobody's asking for, at the cost of another
subject to manage and a decode-re-encode hop in the gateway. Right-sizing rigor to the
boundary is the principle; "everything must be Avro" is cargo culting. (Fair
counterpoint an interviewer might raise: if a second consumer of delay scores appeared,
we'd want the schema discipline — and migrating a topic's format later is painful, as
§4's JSON→Avro scar proves. The choice is defensible, not free.)

**If this service stops:** nothing else crashes. The dashboard's delay panel freezes on
the last scores (the gateway's Redis `delay:*` keys have no TTL — a deliberate choice,
"stale score" beats "empty panel" for this data). On restart, the app rebuilds any lost
window state from its changelog topics and resumes from committed offsets; at most it
re-emits a window or two (at-least-once), which downstream absorbs because Redis writes
are keyed overwrites — the *pipeline* is idempotent even though the processor isn't
exactly-once.

**If it produces *wrong* data:** more interesting failure mode. The dashboard displays
whatever arrives — there's no sanity bound on scores downstream. The historical
headway-inflation bug in the *loader* (see gtfs-loader's doc, when written) showed
exactly this shape: everything flowed, all green, numbers subtly nonsense. Stream
pipelines fail loudest when they fail *silently correct-looking*.

---

## 4. Bugs and decisions from actual project history

### 4.1 The logger that silently vanished (SLF4J 1.x vs 2.x binding)

- **Symptom.** First containerized run: the app started, consumed, seemingly worked —
  but emitted *three lines total*: `SLF4J: Failed to load class
  "org.slf4j.impl.StaticLoggerBinder"` / `Defaulting to no-operation (NOP) logger`.
  Every log statement in the app went to a black hole. Not a crash — worse: an app
  that runs fine and tells you nothing.
- **Investigation.** The pom had `slf4j-simple` 2.0.x (the *provider*), so why no
  binding? Because the `slf4j-api` actually on the classpath was **1.7.36**, pulled in
  transitively by kafka-streams. SLF4J changed its provider-discovery mechanism between
  1.x (looks for `StaticLoggerBinder` class) and 2.x (uses `ServiceLoader`): a 1.x API
  cannot find a 2.x provider. Maven's "nearest wins" resolution had quietly picked
  Kafka's 1.7 API over our intent.
- **Fix.** Declare `slf4j-api:2.0.12` *explicitly* alongside `slf4j-simple:2.0.12` —
  a direct dependency always beats a transitive one, forcing the API and provider onto
  the same major version.
- **Lesson.** *Logging facades bind at runtime, and the API and provider must agree on
  the discovery mechanism* — matched major versions, always declared together. More
  generally: transitive dependency resolution is a silent participant in every build;
  when behavior differs from the pom's apparent intent, `mvn dependency:tree` before
  theorizing. This bug also had a multiplier: **no logs means every subsequent bug is
  harder**, which is exactly what happened — the next crash (4.2) had to be diagnosed
  with logging already broken. Fix observability first.

### 4.2 `NoSuchFieldError` at runtime: the Jackson version clash

- **Symptom.** The Streams thread died mid-processing with
  `java.lang.NoSuchFieldError: READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE`, the
  handler chose `SHUTDOWN_CLIENT`, and the app transitioned `RUNNING → PENDING_ERROR →
  ERROR`. A linkage error, not an exception from logic.
- **Investigation.** That constant lives in `jackson-databind`'s
  `DeserializationFeature`. `NoSuchFieldError` means: some class was *compiled* against
  a Jackson version where the field exists, but *ran* against one where it doesn't —
  two different Jackson generations were on the classpath simultaneously. Cause: the pom
  had explicitly pinned `jackson-databind:2.16.1` ("newer is better," reflexively),
  while Avro and the Confluent serializer transitively brought `jackson-core` 2.14.x.
  Maven happily mixed them: databind 2.16 calling into core 2.14.
- **Fix.** Delete the pin entirely. Let the Avro/Confluent ecosystem's transitives pick
  one internally-consistent Jackson generation. The pom now carries a comment where the
  dependency used to be — a tombstone so nobody "helpfully" re-adds it.
- **Lesson.** *Multi-artifact libraries (jackson-core/databind/annotations; netty;
  grpc) must move in lockstep — pinning one member desynchronizes the family.* Either
  pin all of them via a BOM, or pin none and trust the framework's tested set.
  `NoSuchFieldError`/`NoSuchMethodError` at runtime is almost always this exact disease.
  And: upgrading a dependency without a reason is not hygiene, it's risk.

### 4.3 `UnsatisfiedLinkError`: RocksDB meets Alpine

- **Symptom.** With logging fixed and Jackson healed, the app ran until the first
  windowed aggregate tried to write state, then:
  `java.lang.UnsatisfiedLinkError: /tmp/librocksdbjni....so: Error loading shared
  library libstdc++.so.6: No such file or directory`. Note *when*: not at startup — at
  first state-store open, minutes in. Everything stateless had worked.
- **Investigation.** The stack trace pointed at `org.rocksdb.NativeLibraryLoader`:
  Kafka Streams' state stores are RocksDB, a C++ database, loaded via JNI from a
  bundled `.so`. That library dynamically links `libstdc++` — the C++ standard library
  runtime — which glibc-based images have and **Alpine (musl-based, minimal) does
  not**.
- **Fix.** One Dockerfile line: `RUN apk add --no-cache libstdc++`, with a comment
  explaining why. (The API gateway's Dockerfile pointedly *omits* it, with the inverse
  comment: plain consumers, no Streams state, no RocksDB.)
- **Lesson.** *"Pure Java" frameworks may carry native code in their trunk; JNI failures
  surface at first use, not at startup, so smoke tests must exercise the stateful path.*
  Alpine's tiny images are bought with a nonstandard libc — every JNI-bearing dependency
  (RocksDB, Netty's native transports, Snappy) is a potential landmine. This lesson was
  *pre-paid* for the next two services: crowding-estimator and ripple-detector shipped
  with the fix from day one, which is what a lesson learned is supposed to look like.

### 4.4 Decision: plain Kafka Streams, not Spring Boot / spring-kafka

- **The choice.** This service (and the other two processors) are plain `main()` Maven
  apps; only the API gateway is Spring Boot.
- **Reasoning.** `spring-kafka`'s `@KafkaListener` model is *per-record callbacks* — great
  for "receive message, do side effect" (exactly what the gateway does), but it has no
  windows, no stateful aggregation, no repartitioning; you'd hand-build all of Block 5
  on top of it, badly (buffering windows in a `ConcurrentHashMap` that dies with the
  pod, no changelog recovery, manual timers for window close…). Kafka Streams *is* that
  machinery. Spring can host a Streams topology (`spring-kafka` has support), but what
  it adds — DI, actuator, autoconfig — wasn't needed for a process with one wiring
  path, while adding startup weight and another layer to debug through. The 4-second
  container startup and ~40 MB smaller jar are minor perks; the honest main reason is
  *nothing here needed Spring*.
- **Lesson.** *Choose the runtime model by the shape of the computation:* per-event side
  effects → plain consumer/listener; time-windowed stateful transforms → a streams
  framework. And frameworks are per-service decisions in a microservice world, not
  one-size-fits-the-repo.

### 4.5 Decision: 5-minute tumbling window, 30-second grace

- **The dial positions.** Window size trades *statistical mass vs freshness*: 1-minute
  windows would see 0–2 vehicles on most routes (headway estimates of "5/1" quantization
  garbage) and emit 5× the messages; 15-minute windows would smooth real incidents into
  invisibility — a 10-minute gap (a genuine problem for riders) vanishes inside a
  15-minute average. Five minutes matches the phenomenon's timescale: TTC headways are
  2–15 minutes, so a 5-minute bucket usually catches 1–3 vehicles per direction and a
  disruption shifts the count visibly. It's also the granularity at which "current
  delay" feels current on a dashboard.
- **Grace at 30s** (vs Streams' old 24h default): our event timestamps are
  producer-assigned and the producer polls every 15s, so records arrive nearly in
  order; almost nothing is later than one poll cycle. Combined with suppression, grace
  directly *adds to result latency* (result at window_end + grace), so it's kept just
  above the real out-of-orderness bound.
- **Lesson.** *Window size should match the timescale of the phenomenon you're
  measuring, and grace should match your measured out-of-orderness — both are data
  questions, not framework defaults.*

### 4.6 The silent drop for unknown routes — accepted gap, with a caveat

- **Behavior.** A window for a route with no `scheduled_headways` row → `toPrediction`
  returns null → filtered out → no output at all for that route. Happens for real
  routes: night buses in hours the weekday-service computation didn't cover, brand-new
  or renamed routes, and the odd garbage route id from the feed.
- **Why accepted.** The score is *defined as* a comparison; without a baseline there's
  nothing true to emit. Fabricating a default schedule (say, 10 minutes) would produce
  confident-looking wrong scores — strictly worse than absence, because downstream can
  render "no data" honestly but can't detect "plausible lie."
- **The caveat that makes it a half-bug.** The drop is logged at `debug` — invisible in
  normal operation. When the *same* pattern came up in the crowding estimator two
  sessions later, the session spec explicitly demanded WARN logging for it, citing this
  exact lesson. The principle earned there: **dropping data can be correct; dropping it
  invisibly never is.** This file still logs at debug; retrofitting it to WARN is a
  known 1-line improvement nobody has prioritized. Being able to say "correct decision,
  imperfect observability, and here's the diff I'd make" is the honest version.

### 4.7 Inherited scar: the JSON records already on the topic

Covered in Block 3, listed here because it shaped a config line: this service's first
start crashed into pre-Avro JSON records left on `vehicle-positions` from sessions 1–2.
`LogAndContinueExceptionHandler` is permanent armor from that incident. **Lesson:** *a
topic's history is part of its schema.* Format migrations must plan for the bytes
already written — version the topic, set a starting offset policy, or make consumers
tolerant. We chose tolerance; the cost is that genuinely-corrupt records are skipped
with only a log line forever after.

---

## 5. Concepts glossary (as used in this service)

**Kafka Streams (vs a plain consumer).** A plain `KafkaConsumer` gives you a loop of
records and nothing else — state, windowing, fault-tolerant aggregation are your
problem. Kafka Streams is a library that runs *inside* your JVM and provides those as
declarative operators, storing state locally (RocksDB) with durability via Kafka
itself (changelog topics). No separate cluster: this service is just `java -jar` in a
container. Here it turns "group 100K daily positions by route into 5-minute buckets and
count distinct vehicles" into nine fluent lines.

**Topology.** The dataflow graph a Streams app declares — sources → processors → sinks.
`buildTopology()` returns one; `KafkaStreams` executes it. Being a *description* (built
before start), it's separately testable with `TopologyTestDriver` without a broker —
a rough edge here: this repo has no such tests, verification was end-to-end only.

**KStream vs KTable.** Two readings of a topic. KStream: every record is an independent
event (this service's input — each position report matters for counting). KTable: the
topic is a changelog and only the latest value per key matters (how the ripple detector
reads alerts). The windowed aggregate in the middle of our topology is actually a
KTable (continuously-updated per-window results) that `toStream()` converts back to
events once suppression finalizes them.

**Serde.** SERializer + DEserializer bundled — Streams needs one wherever objects cross
a byte boundary: input topic (Avro serde), state store (`StringSetSerde` — the
non-obvious one), and output topic (String serde). Miss one and you get runtime
ClassCastExceptions, not compile errors.

**Tumbling window.** Fixed-size, non-overlapping, epoch-aligned time buckets; each event
belongs to exactly one. Contrast hopping (overlapping — smoother but N× state and
output) and session windows (gap-based). Chosen here because the product question is
"one delay number per route per 5 minutes."

**Grace period.** Extra time a window accepts late events after its nominal end, judged
by stream time (max event timestamp seen), not wall clock. 30s here, matched to the
producer's 15s poll cadence. With suppression, grace is a direct latency tax: results
appear at window_end + grace.

**Suppression.** Operator that withholds windowed-aggregate updates until the window
closes, emitting one final result instead of a stream of intermediates. Its gotcha:
emission is driven by stream time, so if input stops, the final window never emits —
"suppression needs a heartbeat." Safe here because transit data never stops; a
known trap on quiet topics.

**State store / RocksDB.** Where aggregation state lives: an embedded key-value database
(RocksDB, C++ via JNI) on local disk, one instance per partition, fronting a changelog
topic for durability. Local = fast (no network hop per update); changelog = a failed
instance's replacement rebuilds the store by replaying Kafka. The reason our Alpine
image needs `libstdc++` and the reason a Streams app is stateful-but-disposable.

**Changelog topic.** The auto-created internal topic (`<application.id>-...-changelog`)
mirroring every state-store write. It's compacted, so it stays bounded by keyspace size
rather than write volume. You can watch these appear on the broker — this app's
aggregate store created one, and its Avro-serialized repartition sibling even
auto-registered a schema in the registry.

**Repartition topic.** Created automatically when you change the grouping key
(`groupBy` route on a stream keyed by vehicle): records are written back to Kafka keyed
by the new key so all records per key colocate on one partition/instance. Cost: an
extra full write+read of the stream through the broker — the hidden price tag on
re-keying, and why key choice at the *producer* matters so much.

**application.id / consumer group.** One string that is simultaneously the consumer
group id (instances of this app share partitions and failover automatically), the
prefix for all internal topics, and the local state namespace. Two Streams apps must
never share one; renaming it orphans state and offsets.

**Stream time vs wall-clock time.** Streams advances its clock from event timestamps,
not the system clock. Window membership, lateness, and suppression all use stream time.
Consequence used deliberately here: `hourOfDay` derives from the window's own start
timestamp, so replaying old data compares against the schedule *for that data's hour* —
replay-correct by construction.

**HikariCP / connection pooling.** Opening a Postgres connection is a multi-round-trip
operation; a pool keeps a few open and lends them out. HikariCP is the de facto Java
pool (Spring Boot's default, used standalone here). Sized to 2 — not the default 10 —
because arithmetic said ~1 query every 2 seconds at peak; pool size should come from
expected concurrency, not folklore.

**At-least-once vs exactly-once.** This app runs the default `at_least_once`: after a
crash between processing and offset commit, some records are reprocessed and a window's
result may be emitted twice. Streams *offers* `exactly_once_v2` (Kafka transactions:
atomically commit state changelog + output + offsets), at a throughput/latency cost and
with the caveat that it protects the Kafka-to-Kafka path only — our JDBC lookup and the
gateway's Redis writes sit outside the transaction anyway. Duplicates here are harmless
by design: the output is keyed and downstream (Redis) does last-write-wins overwrites.
Interview line: *we made duplicates idempotent instead of impossible, because the
idempotence was free and the transactions weren't.*

**JNI (Java Native Interface).** The JVM's mechanism for calling native (C/C++)
libraries. Invisible until it isn't: RocksDB ships as a `.so` inside a jar, extracted
and loaded at first state-store use, dynamically linking against system libraries the
container may not have (`libstdc++` on Alpine — §4.3). "Pure Java" is a spectrum, and
its native tail surfaces lazily.

**Fat jar (assembly plugin).** A single jar containing the app plus every dependency,
with `Main-Class` in the manifest — the plain-Java equivalent of Spring Boot's
repackaged jar, so the container runs `java -jar app.jar` with no classpath management.
Built by `maven-assembly-plugin`'s `jar-with-dependencies` descriptor at `package`.

**avro-maven-plugin (code generation).** Reads `.avsc` schema files at build time and
generates typed Java record classes (`ca.ttc.intelligence.VehiclePosition`) —
compile-time contract enforcement from the same schema file the Go producer loads at
runtime. With `specific.avro.reader=true` on the deserializer, wire bytes decode
directly into these classes. `<stringType>String</stringType>` avoids the
CharSequence/Utf8 equality trap.
