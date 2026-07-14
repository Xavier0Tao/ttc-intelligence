# 03 — Crowding Estimator (Java + Kafka Streams)

> Study notes for `crowding-estimator/`. This service shares its skeleton with the
> delay predictor — same Kafka Streams runtime, same serde setup, same Dockerfile
> pattern, same pom structure. **Read `02-delay-predictor.md` first**; this doc
> deliberately does not re-explain tumbling windows, suppression, serdes, RocksDB,
> changelog/repartition topics, or the SLF4J/Jackson/libstdc++ pitfalls. It focuses on
> what's *different* here — which happens to include the project's best
> "the real world doesn't match the spec" story.

---

## 1. Purpose

The crowding estimator produces a **per-vehicle** crowding guess — LIKELY_CROWDED /
NORMAL / LIKELY_EMPTY / UNKNOWN — with no passenger data at all, by exploiting a piece
of transit domain knowledge: a vehicle running far behind the one ahead of it has had
more time for passengers to accumulate at every stop it's approaching, so **gap ahead ≈
crowding**. Every 5 minutes, per route+direction, it sorts vehicles by how far along
the route they are, measures each one's gap to the vehicle in front, compares that gap
to the scheduled headway, and publishes one JSON estimate per vehicle to
`crowding-estimates` — which the dashboard renders as the colored ring around each
vehicle marker.

---

## 2. Line-by-line walkthrough

Files: `CrowdingEstimatorApp.java` (topology + estimation logic),
`TimescaleDBClient.java` (headway lookup **plus** a bulk trip-direction load),
`VehicleSnapshot.java` + `VehicleMapSerde.java` (state shape), and the same
`AppConfig`/`application.properties`/pom/Dockerfile pattern as the delay predictor.

### Block 1: the heuristic's constants

```java
private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
private static final Duration WINDOW_GRACE = Duration.ofSeconds(30);
private static final ZoneId TORONTO = ZoneId.of("America/Toronto");

private static final double CROWDED_RATIO = 1.5;
private static final double EMPTY_RATIO = 0.5;
```

Window and grace are identical to the delay predictor (same input cadence, same
latency/completeness tradeoff — see 02 §4.5). New here are the classification
thresholds: a vehicle whose gap-ahead is ≥1.5× the scheduled headway is LIKELY_CROWDED,
≤0.5× is LIKELY_EMPTY, between is NORMAL. **Honest assessment:** 1.5 and 0.5 are not
calibrated against any ridership data — they're round numbers encoding "noticeably
worse/better than schedule." The defensible part is the *structure* (normalize by
scheduled headway, so a 10-minute gap is alarming on the 504 but normal on a
half-hourly suburban route); the exact cutoffs are tunable guesses, and saying so beats
pretending there's science behind 1.5.

### Block 2: the startup reference-data load — the direction_id workaround

```java
// The TTC GTFS-RT feed does not populate trip.direction_id, so we
// resolve direction from the static GTFS trips table instead.
Map<String, Integer> tripDirections = db.loadTripDirections();
if (tripDirections.isEmpty()) {
    log.warn("trips table is empty — direction can only come from the feed, "
            + "which the TTC does not populate; most records will be dropped. "
            + "Run `make load-gtfs` to populate it.");
} else {
    log.info("loaded {} trip->direction mappings from TimescaleDB", tripDirections.size());
}
```

and in `TimescaleDBClient`:

```java
public Map<String, Integer> loadTripDirections() {
    Map<String, Integer> directions = new HashMap<>(200_000);
    try (Connection conn = dataSource.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(TRIP_DIRECTIONS_QUERY)) {
        while (rs.next()) {
            directions.put(rs.getString(1), rs.getInt(2));
        }
    } catch (SQLException e) {
        log.error("failed to load trip directions: {}", e.getMessage());
    }
    return directions;
}
```

**What it does.** At startup, pulls the *entire* static-GTFS `trips` table
(trip_id → direction_id, ~126,000 rows for the TTC) into an in-memory `HashMap`.

**Why this exists at all** is §4.1's story in full, but the short version: the spec said
"filter out records where direction_id is null," and it turned out the TTC feed sets
direction_id on *zero* records — the filter as specified would have silently discarded
100% of input and this service would have been an elaborate no-op. The feed *does* send
`trip_id`, and the static schedule knows every trip's direction, so the join recovers
what the feed omits.

**Why a full in-memory load instead of per-record queries.** Arithmetic again: ~1,700
vehicles × every 15s = ~110 lookups/second on the hot path. As SQL queries that's a
constant DB load and a network round trip inside a stream operator; as a HashMap lookup
it's ~50ns. Memory cost: 126K entries of short strings and boxed ints ≈ 15–20 MB —
trivial. The `new HashMap<>(200_000)` presizing avoids rehash-and-copy churn during the
load (same instinct as the Go slice-capacity trick in doc 01).

**The honest rough edges, name them before the interviewer does:**
1. *Staleness*: the map loads once at startup. If `make load-gtfs` refreshes the
   schedule (TTC publishes ~quarterly), running estimators keep the old map until
   restarted. New trip_ids in the live feed that aren't in the map resolve to null →
   those records get dropped (visibly, per Block 3). Acceptable at quarterly cadence;
   would need a refresh timer or a proper stream-table join if schedules churned daily.
2. *Failure softness*: if the DB is down at startup, `loadTripDirections` logs an
   ERROR and returns an *empty map* — the app starts anyway and drops nearly
   everything. The WARN with an actionable remedy ("Run `make load-gtfs`") is the
   mitigation; arguably this should be fail-fast like schema loading is. Defensible
   either way — an empty-but-alive estimator recovers on restart without human
   intervention once the DB returns, a crashed one needs the restart anyway.

**The canonical alternative (know it for interviews):** stream the trips table into a
Kafka topic and join against it as a `GlobalKTable` — every instance gets a full,
continuously-updated local replica, no restart-staleness, no DB dependency at runtime.
We didn't because it requires a schedule→Kafka feed that doesn't exist (the loader
writes to Postgres), and building one to avoid a quarterly restart is poor ROI at this
scale. Same pragmatic-vs-canonical tradeoff as the JDBC lookup in doc 02, one level up.

### Block 3: the topology — same shape as 02, three meaningful differences

```java
builder.stream(inputTopic, Consumed.with(Serdes.String(), vehicleSerde))
        .filter((key, vp) -> vp != null
                && vp.getRouteId() != null && !vp.getRouteId().isEmpty()
                && vp.getVehicleId() != null && !vp.getVehicleId().isEmpty()
                && vp.getCurrentStopSequence() != null
                && resolveDirection(vp, tripDirections) != null)
        .groupBy((key, vp) -> vp.getRouteId() + ":" + resolveDirection(vp, tripDirections),
                Grouped.with(Serdes.String(), vehicleSerde))
        .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE))
        .aggregate(
                HashMap::new,
                (routeDirection, vp, vehicles) -> {
                    VehicleSnapshot previous = vehicles.get(vp.getVehicleId());
                    if (previous == null || vp.getTimestamp() >= previous.timestamp) {
                        vehicles.put(vp.getVehicleId(),
                                new VehicleSnapshot(vp.getCurrentStopSequence(), vp.getTimestamp()));
                    }
                    return vehicles;
                },
                Materialized.with(Serdes.String(), new VehicleMapSerde()))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .flatMap((windowedKey, vehicles) -> buildEstimates(windowedKey, vehicles, db))
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
```

**Difference 1 — the composite grouping key `route + ":" + direction`.** The delay
predictor groups by route alone; this service *must* split directions, because the
gap-ahead concept is directional: an eastbound 504 at stop 12 and a westbound 504 at
stop 14 are not "two stops apart" — they're passing each other on opposite sides of the
street, and computing a gap between them is meaningless. Kafka Streams keys are just
strings/bytes, so "group by two things" is expressed by concatenating into one key —
`"504:0"`, `"504:1"` — which then flows through the repartition topic, the state store,
and the output as a single unit. (The delay predictor arguably *should* do this too —
its both-directions mixing is one of its acknowledged approximations. This service,
built two sessions later, is the more correct pattern.)

`resolveDirection` is called twice (filter, then groupBy) — a tiny redundancy, two map
lookups instead of one. Streams' cleaner fix is `mapValues` to an enriched record first;
not worth the extra class here, but worth noticing.

**Difference 2 — the aggregate keeps the LATEST snapshot per vehicle, not a set.**

```java
// VehicleSnapshot.java — the whole class
public class VehicleSnapshot {
    public int stopSequence;
    public long timestamp;
    public VehicleSnapshot() { }
    public VehicleSnapshot(int stopSequence, long timestamp) { ... }
}
```

The delay predictor only needed "how many distinct vehicles" (a `Set<String>`); this
service needs "where is each vehicle *now*" (a `Map<vehicleId, snapshot>`). Each vehicle
reports ~20 times per 5-minute window as it moves; for gap math you want its most
recent position, so the aggregator overwrites.

The guard `vp.getTimestamp() >= previous.timestamp` is a small but real piece of
distributed-systems hygiene: records within the window are *mostly* in order, but
out-of-order delivery is possible (producer retries, partition interleaving after the
repartition step). Without the check, a late-arriving *older* position would overwrite a
newer one and a vehicle would appear to move backward. One `if` buys out-of-order
tolerance; the naive `put` is wrong in a way that would surface as rare, unreproducible
gap glitches — the worst kind of bug.

**Why `VehicleSnapshot` has public fields and a no-arg constructor:** it's a Jackson
DTO. `VehicleMapSerde` (structurally identical to 02's `StringSetSerde`, ~45 lines)
serializes the whole `Map<String, VehicleSnapshot>` to JSON for the RocksDB store and
its changelog topic. No-arg constructor + public fields = zero annotation Jackson
round-tripping. Same "JSON is a lazy but adequate state encoding" tradeoff as 02.

**Difference 3 — `flatMap` instead of `map`.** The delay predictor emits one record per
window (`map`: 1→1). This service emits one record *per vehicle* in the window
(`flatMap`: 1→N, or 1→0 when the schedule lookup fails — returning an empty list is
also how "skip this window" is expressed). One input (a closed window's vehicle map)
fans out to typically 1–6 output messages, each keyed by `vehicle_id`.

**Why output is keyed by vehicle_id:** the consumer is the API gateway, which writes
each estimate to Redis as `crowding:{vehicle_id}` and the dashboard joins it to the
vehicle marker client-side. Keying by vehicle also means all estimates for one vehicle
land in one partition, ordered — so Redis can never apply them out of sequence.

### Block 4: `buildEstimates` — the actual crowding math

```java
String composite = windowedKey.key();
int split = composite.lastIndexOf(':');
String routeId = composite.substring(0, split);
int directionId = Integer.parseInt(composite.substring(split + 1));
```

Unpacking the composite key. `lastIndexOf` rather than `indexOf` or `split(":")` is
deliberate paranoia: if a route id ever contained a `:`, splitting on the *last* colon
still yields the right direction (the part we control the format of). Cheap defensive
coding at a format boundary we invented ourselves.

```java
OptionalDouble scheduled = db.getScheduledHeadway(routeId, hourOfDay);
if (scheduled.isEmpty()) {
    log.warn("no scheduled headway for route={} hour={}; skipping {} vehicle estimates",
            routeId, hourOfDay, vehicles.size());
    return out;
}
```

Same accepted gap as the delay predictor (no baseline → no output) with one upgrade:
**WARN, not debug, and it counts what was dropped.** This is a lesson from doc 02 §4.6
applied in code — the session spec for this service explicitly demanded it after the
delay predictor's invisible drops. Evolution of a codebase's error-handling culture,
visible in a one-line diff between two otherwise-parallel services.

```java
List<Map.Entry<String, VehicleSnapshot>> sorted = new ArrayList<>(vehicles.entrySet());
sorted.sort((a, b) -> Integer.compare(a.getValue().stopSequence, b.getValue().stopSequence));

// Rough time-per-stop estimate: assume the fleet, spread over
// (maxSeq - minSeq) stops, covers that span in one window length.
// minutes_per_stop = window_minutes / span. This ignores actual travel
// speed and stop spacing — it is only meant to turn "stops of gap"
// into an order-of-magnitude "minutes of gap".
int span = sorted.get(sorted.size() - 1).getValue().stopSequence
        - sorted.get(0).getValue().stopSequence;
Double minutesPerStop = span > 0 ? (double) WINDOW_SIZE.toMinutes() / span : null;
```

**The unit-conversion problem, honestly.** Gaps are naturally measured in *stops*
(difference of `current_stop_sequence`), but the scheduled headway is in *minutes* —
comparing them needs a stops→minutes exchange rate. The estimate used: "the vehicles in
this window collectively span `span` stops; call that 5 minutes of travel." What's wrong
with it (be able to recite this):

- It assumes the fleet's *spatial spread* equals *5 minutes of driving*, which is only
  loosely true — really the span reflects how many vehicles are out and how spaced they
  are, not travel speed.
- Stop spacing varies (downtown stops every 200m, suburban every 500m+), so "stops" is
  an uneven ruler to begin with.
- With 2 vehicles close together, `span` is small → `minutesPerStop` huge → ratios
  explode. Thresholding at ±ratios bounds the damage (everything ≥1.5 is just
  "CROWDED"), but the numeric `gap_ahead_minutes` shipped in the JSON can be silly.

The defense is the code comment itself: it's an *order-of-magnitude converter* feeding a
three-bucket classifier, not a prediction. The correct version — per-vehicle speed from
consecutive positions, or scheduled inter-stop times from `stop_times.txt` — needs
either cross-window state or a much bigger static-data join. Known upgrade path,
deliberately not taken for a v1 heuristic.

```java
Integer gapStops = null;
if (i < sorted.size() - 1) {
    gapStops = sorted.get(i + 1).getValue().stopSequence
            - sorted.get(i).getValue().stopSequence;
}
```

**Gap semantics.** Sorted ascending by stop sequence, vehicle `i`'s "ahead" neighbor is
`i+1` (further along the route). The *lead* vehicle has nobody ahead → `gapStops`
stays null → `crowding_level: "UNKNOWN"`. Same for every vehicle when the window
contains only one (span 0 → `minutesPerStop` null). This null-chain is why UNKNOWN
legitimately dominates the output at night: **an observed real distribution (~11 PM)
was 1742 UNKNOWN / 317 NORMAL / 201 LIKELY_EMPTY / 103 LIKELY_CROWDED** — most
route-direction-windows have one vehicle in evening service, and every window's lead
vehicle is UNKNOWN by construction. If UNKNOWN ever *stopped* dominating overnight,
that would itself be suspicious.

```java
json.putNull("gap_ahead_minutes");
json.putNull("crowding_ratio");
...
json.put("crowding_level", classify(ratio));
```

**UNKNOWN is emitted, not dropped.** Compare with the missing-schedule case (dropped
with WARN): a missing *baseline* means we can't say anything; a missing *gap* is itself
information ("this vehicle leads its route"). The dashboard renders UNKNOWN as a gray
ring rather than no ring — absence-of-signal displayed honestly instead of hidden.
Three-valued honesty: value, unknown-but-present, absent — and each is handled
differently on purpose.

### Block 5: everything this service inherited without re-fighting

The pom is the delay predictor's with names changed — same Confluent repo, same
no-Jackson-pin comment, same slf4j-api+simple 2.0.12 pair, same avro-maven-plugin
reading `../schemas` (it generates *both* `VehiclePosition` and `ServiceAlert` classes
since the plugin compiles the whole shared directory; the unused `ServiceAlert` is
harmless dead weight). The Dockerfile is byte-for-byte the same pattern including
`apk add libstdc++`. `application.properties` differs in three values:

```properties
kafka.application.id=crowding-estimator
kafka.input.topic=vehicle-positions
kafka.output.topic=crowding-estimates
```

Note what `application.id=crowding-estimator` does: this service and the delay
predictor **consume the same topic in different consumer groups**, so each gets the
full stream independently — that's the Kafka fan-out model working as intended (one
write, N independent readers, no coordination between them). Their internal topics and
state directories are likewise namespaced apart.

This "boring by inheritance" quality is itself the lesson: the first Streams service
paid for three crashes (SLF4J, Jackson, libstdc++); this one started clean on its first
build because those fixes shipped in its scaffold. §4.2.

---

## 3. How this service connects to the rest of the system

**Consumes:** `vehicle-positions` (Avro, keyed by vehicle_id) — same topic as the delay
predictor, independent consumer group. Crucially, it consumes fields the schema only
gained in v2 (`trip_id`, `current_stop_sequence`) — this service is *why* that schema
evolution happened, and it's the only consumer that would break if those fields
regressed to null (its filter would silently drop everything… visibly, thanks to the
startup WARN and per-window logs).

**Reads from TimescaleDB:** two tables, two access patterns —
`trips` (bulk-loaded once at startup into memory, ~126K rows: the direction resolver)
and `scheduled_headways` (queried per window close, pooled JDBC — identical client
class to 02). Both are populated by `gtfs-loader`; this service is the reason the
loader grew the `trips` table at all.

**Produces:** `crowding-estimates` — plain JSON (same Avro-in/JSON-out boundary
reasoning as 02 §3: single known consumer, leaf topic, JavaScript destination), one
message per vehicle per route-direction window, keyed by `vehicle_id`:

```json
{"vehicle_id":"4218","route_id":"504","direction_id":0,
 "gap_ahead_minutes":9.2,"crowding_ratio":1.84,
 "scheduled_headway_minutes":5.0,"crowding_level":"LIKELY_CROWDED",
 "window_end":1719530700000,"computed_at":1719530712000}
```

**Downstream:** the API gateway writes each estimate to Redis (`crowding:{vehicle_id}`,
10-minute TTL — deliberately longer than the 5-minute window cadence so a crowding
value survives until the next window replaces it) and broadcasts a `crowding_update`
over WebSocket; the dashboard merges it into the vehicle's map marker as the ring
color. **If this service stops:** rings fade to gray (UNKNOWN) as the Redis keys
expire over ~10 minutes; nothing else notices. Map, delays, and alerts all keep
working — the panels degrade independently, which is the whole microservice/topic
decoupling argument in one observable behavior.

**If it produces wrong data:** gray/red/blue rings lie silently — there's no
ground-truth check anywhere downstream (no APC passenger counts to validate against).
This is the least-verifiable output in the system, and honesty about that is the
answer: the output is labeled "LIKELY_", the dashboard treats it as a hint, and nothing
operational depends on it.

---

## 4. Bugs and decisions from actual project history

### 4.1 The direction_id that never comes (the big one for this service)

- **Symptom.** None at first — that's the point. The session spec said: filter out
  records where `current_stop_sequence` *or* `direction_id` is null, group by
  route+direction. Before building, a schema-evolution verification step sampled the
  freshly-added v2 fields on live data: `trip_id` populated, `current_stop_sequence`
  populated… `direction_id` null. On every record sampled — 0 of 60 non-null.
- **Investigation.** Widened the sample to rule out luck (still zero), checked the raw
  GTFS-RT protobuf semantics (TripDescriptor.direction_id is *optional*, and the TTC
  simply doesn't send it — permitted by spec). Conclusion: the filter as specified
  would drop 100% of input. The service would deploy, run green, log nothing
  alarming, and emit nothing, forever.
- **Root cause.** Spec written against the GTFS-RT *standard*; the *implementation* of
  that standard by this particular agency omits an optional field. Optional means
  optional.
- **Fix — and the decision about *where* to fix it.** Three options were on the table:
  1. *Enrich in the ingestion service* (look up direction there, stamp it into the
     Avro record). Rejected: ingestion stays a faithful mirror of the feed — inventing
     data at the edge means every consumer inherits the inference, including future
     ones that might want to know what the feed actually said, and ingestion would grow
     a DB dependency it doesn't otherwise need.
  2. *Fix in the consumer* (this service): resolve direction from static GTFS at use
     time. Chosen — the workaround lives exactly where the need lives.
  3. *Drop direction entirely* (group by route only). Rejected: gaps across mixed
     directions are meaningless; the output would be confidently wrong.
  Concretely: `gtfs-loader` grew a `trips` table (all 126K trips, deliberately *not*
  filtered to weekday service, since live trip_ids can belong to any service day), and
  this service bulk-loads it and falls back `feed direction → trip lookup → drop`.
  The filter drops only records where *both* sources fail.
- **Lesson.** *Validate upstream data against reality before building logic on its
  contract — optional fields in third-party feeds are absent until proven present.*
  And the placement principle: **repair data as close to where it's needed as possible,
  keep the system-of-record layer honest about what it actually received.**

### 4.2 The bugs that didn't happen (inherited fixes as evidence of process)

- **Observation, not symptom:** this service compiled and ran correctly on its first
  container build — logs present, no `NoSuchFieldError`, no `UnsatisfiedLinkError` when
  the first window store opened.
- **Why that's worth a section:** its scaffold carried all three of the delay
  predictor's hard-won fixes from day one (slf4j-api pinned beside slf4j-simple; no
  Jackson pin, with the tombstone comment; `libstdc++` in the Dockerfile with its
  comment). The session prompt even listed them as known pitfalls to apply proactively.
- **Lesson.** *A bug is only fully fixed when the **next** service can't reintroduce
  it.* Comments-at-the-scar and prompt/checklist propagation are the lightweight
  version; a shared parent pom or base image would be the industrial version (and is
  the obvious refactor if a fourth Streams service ever appears — three copies of an
  identical pom is right at the duplication threshold).

### 4.3 Decision: WARN loudly on the missing-schedule drop

Covered in Block 4 — the same skip-if-no-baseline behavior as the delay predictor, but
logged at WARN with a dropped-count, *because* the delay predictor's debug-level silence
had already cost debugging time. The transferable rule as it ended up phrased in this
project: **dropping data can be correct; dropping it invisibly never is.** (The delay
predictor itself was never retrofitted — the inconsistency between the two services is
real, known, and a one-line fix that keeps not being the priority. Honest state of most
codebases.)

### 4.4 Decision: latest-position-per-vehicle with an out-of-order guard

The aggregate could have kept the *first* position per vehicle (cheaper: `putIfAbsent`)
or *all* positions (richer: could compute per-vehicle speed). Latest-wins matches the
question being asked — "where does everyone stand at window close?" — and stays O(fleet)
in state size. The `timestamp >= previous.timestamp` comparison guards the overwrite
against out-of-order arrivals. **Lesson:** *when a reduce keeps "the latest," define
latest by event time, not arrival order — the two disagree exactly often enough to
create unreproducible bugs.* (Also note `>=` not `>`: equal timestamps take the newer
arrival, a harmless tie-break that avoids stalling on the TTC's 1-second timestamp
granularity.)

### 4.5 Decision: emit UNKNOWN, don't fabricate and don't drop

The lead vehicle of every window, and every vehicle in a single-vehicle window, gets
`crowding_level: "UNKNOWN"` with null gap/ratio rather than either (a) being dropped or
(b) being defaulted to NORMAL. Dropping would make the dashboard's rings flicker in and
out per window; defaulting would assert knowledge that doesn't exist. The observed
distribution (UNKNOWN dominating off-peak — 1742 of ~2360 estimates one late evening)
is the heuristic honestly reporting its own blind spots. **Lesson:** *"I don't know" is
a first-class output value; systems that can't say it say wrong things instead.*

### 4.6 Inherited incident: the producer throughput bug capped this service's coverage

- **Symptom (retroactive).** For its first two sessions of life, this service only ever
  produced estimates for the ~120 vehicles that existed in the pipeline — because the
  ingestion service's 1-msg/sec producer bug (doc 01 §4.6) meant only ~120 vehicles'
  positions flowed at all. Nothing in *this* service was wrong; its input was starved.
- **How it surfaced.** When the producer fix landed, this service's output volume and
  the dashboard's ring-color variety jumped in the same instant — from a handful of
  gray rings to the full 300+ colored distribution — with zero changes here.
- **Lesson.** *A stream processor's output quality is bounded by its input's
  completeness, and it has no way to know its input is incomplete.* Volume expectations
  ("~1,700 vehicles should yield ~N estimates per window") belong in monitoring, not in
  heads. When debugging a pipeline, suspect the stage *before* the one that looks quiet.

### 4.7 Small decision: the ignored `occupancy` field

The Avro schema carries an `occupancy` field straight from the feed (the GTFS-RT
`OccupancyStatus` enum), and this service — whose entire purpose is crowding —
deliberately ignores it. Reasoning: sampled values from the TTC feed were monotonous
(overwhelmingly `EMPTY` regardless of time or route), suggesting a default/stub rather
than real APC sensor data, and the project brief was explicitly to *infer* crowding
from headway geometry. The field still flows through the pipeline (ingestion mirrors
the feed; the dashboard tooltip even shows it) so if the TTC ever starts populating it
properly, validating this service's heuristic against it becomes possible without any
schema work. **Lesson:** *carry upstream signals you don't trust yet rather than
stripping them — future-you gets a validation dataset for free.*

---

## 5. Concepts glossary (delta over doc 02)

Shared concepts — Kafka Streams itself, topology, KStream/KTable, serdes, tumbling
windows, grace, suppression, state stores/RocksDB, changelog and repartition topics,
at-least-once, JNI, fat jars, avro codegen — are in `02-delay-predictor.md` §5 and are
used identically here. New or meaningfully different in this service:

**Composite key.** Kafka keys are opaque bytes, so "group by (route, direction)" is
expressed by string-concatenating into one key (`"504:1"`). Everything keyed downstream
— repartition topic, state store, suppression buffer — treats it atomically. The
parsing back out (`lastIndexOf(':')`) is the tax; the discipline is that whoever
invents a composite format owns both encode and decode, and picks separators that can't
appear in the components (or splits defensively, as here).

**`flatMap` (1→N emission).** Where `map` transforms each record into exactly one
output, `flatMap` returns a *list* — zero, one, or many. Used here to explode one
closed window (a map of vehicles) into per-vehicle messages, and — the less obvious
use — returning an empty list is how an operator says "emit nothing for this input"
(the missing-schedule skip). Same concept as Java Streams' `flatMap`, applied to
infinite streams.

**Fan-out via consumer groups.** This service and the delay predictor both consume
`vehicle-positions` in full, independently, because they use different
`application.id`s (→ different consumer groups). Kafka topics are pub-sub at the group
level: adding a new consumer group costs the broker almost nothing and requires no
coordination with existing consumers. This is the architectural reason the platform
could grow one processor per session without ever touching the ingestion service.

**Reference data / cache warming.** The trips map is classic *reference data*: large
relative to a request, tiny relative to RAM, changes rarely, read constantly. The
pattern used — bulk-load at startup, hold in memory, accept restart-staleness — is the
simplest of the standard options (others: periodic refresh, cache-aside with TTL,
GlobalKTable/CDC replication). Interview framing: pick by *change rate* (quarterly →
startup load wins) and *failure mode* (stale beats unavailable here).

**Out-of-order events.** In any partitioned/retried system, arrival order ≉ event
order. The defenses in this one service, smallest to largest: the timestamp-compare
overwrite guard (per-vehicle), the window grace period (per-window), and event-time
processing generally (Streams' clock advances on record timestamps, not wall time).
None of these are exotic — the discipline is remembering that *every* "keep the latest"
operation needs a definition of latest.

**Heuristic classification with normalized thresholds.** The ratio
`gap_ahead / scheduled_headway` normalizes a raw measurement by context before
thresholding — a 9-minute gap is 1.8× on a 5-minute-headway route (crowded) but 0.3× on
a half-hourly one (empty). Normalizing-then-thresholding is the difference between a
tunable heuristic and a pile of per-route magic numbers; the thresholds themselves
(1.5/0.5) remain admitted guesses pending any ground truth to calibrate against.

**Three-valued output (value / UNKNOWN / absent).** This service distinguishes "here's
the estimate," "the vehicle exists but its crowding is unknowable" (emitted UNKNOWN,
null fields), and "this window can't be scored at all" (dropped, WARN). Collapsing
these — dropping unknowns, or defaulting them — is how dashboards end up quietly lying.
The downstream contract (gray ring vs no ring) preserves the distinction all the way to
the pixel.
