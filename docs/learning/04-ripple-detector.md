# 04 — Ripple Detector (Java + Kafka Streams, KTable edition)

> Study notes for `ripple-detector/`. Structurally this is the odd one out among the
> three Streams services: **no windows, no aggregation, no suppression** — it's a
> KTable-driven enrichment join against geospatial reference data. Don't map it onto
> doc 02's skeleton; only the plumbing (Dockerfile pattern, HikariCP setup, the
> Avro serde double-cast, the SLF4J/Jackson/libstdc++ inheritances) is shared, and
> those are cross-referenced rather than repeated.

---

## 1. Purpose

When a subway station loses service, the disruption doesn't stay on the subway: the
displaced riders walk upstairs and cram onto whatever buses and streetcars serve that
station, so a Line 1 closure at Bloor-Yonge *predictably* degrades routes 97, 300, and
320 minutes later. The ripple detector makes that second-order effect a first-class
event: it watches `service-alerts`, and for every alert touching known subway stations
it emits one **cascade alert per surface feeder route** to `ripple-alerts` —
"route 97 near Bloor-Yonge: likely crowding increase, cascaded from alert X."

It's the most conceptually interesting service in the system for three stacked reasons:
its input is *state, not events* (alerts are long-lived objects that update and resolve,
hence the KTable), its enrichment data is *derived geometry* (which routes feed which
stations was computed from GTFS coordinates, not hand-maintained), and its output is
*a prediction about a different entity than its input* — it consumes subway alerts and
emits statements about bus routes. Every other service transforms its input; this one
reasons across a relationship.

---

## 2. Line-by-line walkthrough

Files: `RippleDetectorApp.java` (topology + cascade logic), `TimescaleDBClient.java`
(the feeder lookup), `FeederRoute.java` (a Java record), plus the standard
`AppConfig`/`application.properties`/pom/Dockerfile (pom and Dockerfile are the doc-02
pattern verbatim — Confluent repo, no-Jackson-pin tombstone comment, slf4j 2.0.12 pair,
avro-maven-plugin on `../schemas`, `libstdc++` in the runtime image).

### Block 1: the topology — four lines doing something unusual

```java
builder.table(inputTopic, Consumed.with(Serdes.String(), alertSerde))
        .toStream()
        .flatMap((alertId, alert) -> buildRipples(alertId, alert, db))
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
```

**`builder.table(...)` — the line the whole service pivots on.** Everywhere else in
this repo, topics are consumed with `builder.stream(...)`: every record is an event,
all of them matter. `builder.table(...)` reads the *same kind of topic* through a
different lens: the topic is interpreted as a **changelog** — a history of updates to a
keyed table — and Streams materializes it into an actual local table (a state store)
where each key holds only the *latest* value. Record for alert X arrives → the table's
row for X is upserted. A record with a null value (a *tombstone*) arrives → the row is
deleted.

This is **table-stream duality**, the single most quotable Kafka Streams idea: a table
is a stream of its own changes, and a stream of keyed updates *is* a table if you only
keep the latest per key. The same bytes on the same topic support both readings; you
pick the one matching your question. Here the question is "what is the current state of
alert X?" — table semantics — because the ingestion service re-publishes an alert
whenever its *content* changes, and processing a superseded version of an alert would
emit ripples about stale reality.

**Why this matters concretely (the load-bearing part):** the upstream topic is
compacted, but compaction is *eventual* (§4.1). At any moment the log can legitimately
contain three versions of alert X. A `stream()` consumer would process all three and
emit three waves of ripples; the `table()` consumer processes each upsert as it arrives
but always *as an update to one row* — and since the output key is deterministic per
(alert, route) (§4.5), reprocessing produces overwrites, not duplicates. Correctness
here comes from the table semantics + deterministic keys, **not** from compaction ever
actually running.

**`.toStream()`** converts the table's *change events* back into a stream: every upsert
to the table (new alert, changed alert, tombstone) flows through once as a record. So
the pipeline reads as: "whenever any alert's current state changes → recompute its
ripples." Note there's no `Materialized.as(...)` name given to the table — Streams
still creates the backing state store (and hence RocksDB and its changelog topic; the
`libstdc++` requirement applies to this service too even though it looks stateless),
it's just anonymous.

**`.flatMap(...)`** — 1→N as in doc 03: one alert update fans out to zero ripples (not
subway-related, or tombstone), or one per feeder route (typically 3–5, e.g. the
Bloor-Yonge/St George demo produced 4).

### Block 2: `buildRipples` — tombstones first

```java
List<KeyValue<String, String>> ripples = new ArrayList<>();
if (alert == null) {
    // Compacted-topic tombstone: the alert was deleted upstream.
    return ripples;
}
```

**What it does.** On a KTable, deletion is expressed as a record with a null value.
`toStream()` delivers those too, with `alert == null`. Today nothing upstream sends
tombstones (the ingestion service never deletes alerts — §4.8), so this branch is
armor for a message type that doesn't yet exist.

**Why it's still the right code:** consuming a KTable and *not* handling null values is
a latent NullPointerException scheduled for whenever deletion is implemented. The
honest flip side: returning an empty list means a tombstoned alert's *existing ripples
are not retracted* — the correct behavior would be to emit tombstones for each derived
`ripple_id`. That's the §4.8 known gap, visible right here as a `return` that should
someday be a loop.

### Block 3: the join — DB lookup by station ids

```java
List<FeederRoute> feeders = db.getFeedersForStations(alert.getAffectedStopIds());
if (feeders.isEmpty()) {
    // Expected for alerts that aren't about subway stations (surface
    // detours, elevator outages at unlisted stops, weather, ...).
    log.warn("alert {} matched no stations in station_feeder_routes "
                    + "(affected_stop_ids={}); no ripples emitted",
            alertId, alert.getAffectedStopIds());
    return ripples;
}
```

and the client side:

```java
private static final String FEEDERS_QUERY =
        "SELECT station_id, station_name, route_id, distance_meters "
                + "FROM station_feeder_routes WHERE station_id = ANY (?)";
...
stmt.setArray(1, conn.createArrayOf("text", stationIds.toArray()));
```

**What it does.** The alert's `affected_stop_ids` (GTFS stop ids the TTC attached to
the alert) are matched against `station_feeder_routes.station_id` — the precomputed
table of "surface routes within 300m of each subway station." One round trip via
Postgres's `= ANY(array)` instead of N queries or an `IN` clause built by string
concatenation (which is how SQL injection happens; the array parameter keeps it fully
parameterized with a variable-length list).

**The semantics of an empty result are "not a subway alert," not "error."** Most real
TTC alerts are surface detours, elevator outages, or blanket weather advisories whose
stop ids (if any) aren't subway stations. Zero ripples is the *correct output* for
them. It still logs at WARN — see §4.7 for why that convention exists and what it
caught elsewhere.

**`FeederRoute` is a Java `record`:**

```java
public record FeederRoute(String stationId, String stationName, String routeId, double distanceMeters) { }
```

One line = immutable value class with constructor, accessors, equals/hashCode —
Java 17's answer to a lombok DTO. Worth knowing cold as "modern Java" signal in
interviews.

### Block 4: dedupe, exclude, emit

```java
// The subway route the alert is about must not appear as its own feeder.
Set<String> alertRoutes = new HashSet<>(alert.getAffectedRouteIds());

// One ripple per feeder route; when a route feeds several affected
// stations, keep the station where it comes closest.
Map<String, FeederRoute> byRoute = new HashMap<>();
for (FeederRoute feeder : feeders) {
    if (alertRoutes.contains(feeder.routeId())) {
        continue;
    }
    FeederRoute existing = byRoute.get(feeder.routeId());
    if (existing == null || feeder.distanceMeters() < existing.distanceMeters()) {
        byRoute.put(feeder.routeId(), feeder);
    }
}
```

Two pieces of join hygiene, each earned by thinking through a concrete case:

1. **Self-exclusion.** A Line 1 alert lists route "1" in `affected_route_ids`; if
   route 1 also appeared in the feeder table near its own stations (it doesn't today,
   but nothing structurally prevents a subway route being within 300m of a station —
   they all are), the output would include "route 1 is affected by the route 1 alert."
   The `alertRoutes` set filters the alert's own routes out of its cascade.
2. **Dedupe to the closest station.** The demo alert covers Bloor-Yonge *and*
   St George; route 300 feeds both. Without the dedupe, ripple_ids
   `X-300` would be emitted twice per update with different `affected_station` fields —
   same key, racing payloads. The map keyed by route with min-distance wins gives one
   ripple per (alert, feeder route), attributed to the station where the route comes
   closest. Observed in verification: 5 matched station rows → 4 ripples, because 300
   collapsed to its Bloor-Yonge row (78.7m beats St George's).

```java
String rippleId = alertId + "-" + feeder.routeId();
...
json.put("predicted_impact", "LIKELY_CROWDING_INCREASE");
json.put("header_text", "Feeder route " + feeder.routeId() + " near "
        + feeder.stationName() + " station: " + alert.getHeaderText());
...
ripples.add(KeyValue.pair(rippleId, json.toString()));
```

**The deterministic key** `alertId + "-" + feeder.routeId()` is §4.5's subject — it's
what makes the whole reprocess-happily design hold together. `predicted_impact` is a
constant: this v1 has exactly one prediction type, and encoding it as a field (rather
than implying it) leaves room for graduated severities later without a format change.
`header_text` prefixes the original alert's human text with feeder context, so the
dashboard's feed can show a self-explanatory sentence without joining back to the
source alert.

**Honest rough edge:** `detectedAt = System.currentTimeMillis()` is *processing* time.
If this service replays the alerts topic from scratch (new `application.id`, disaster
recovery), old alerts re-cascade with fresh `detected_at` timestamps — the dashboard
would briefly show months-old disruptions as "just detected." Downstream TTLs paper
over it. Event-time discipline (doc 02) would use the alert's own timestamp; this
field predates that lesson being fully internalized.

### Block 5: what's deliberately absent

Compare this topology to docs 02/03: no `groupBy` (no repartition topic — the input is
already keyed by `alert_id`, which is exactly the table key we want; keying decisions
made at the *producer* paying off at the *consumer*), no windows (alerts aren't
time-bucketed phenomena; each update should cascade immediately), no suppression
(nothing to hold back — there's no partial aggregate). Alert volume is ~40 active / a
few updates per minute, so the per-update DB lookup that would be unacceptable on
`vehicle-positions` (~110 records/sec) is trivially fine here. **Same architecture
family, completely different operating point — matching the topology to the data's
shape and volume is the design skill on display.**

---

## 3. How this service connects to the rest of the system

**Consumes:** `service-alerts` — Avro `ServiceAlert` records keyed by `alert_id`,
1 partition, `cleanup.policy=compact`. Written by the Go ingestion service's alerts
poller (real TTC alerts, content-fingerprint deduped) and by `scripts/inject_alert.py`
(demo alerts through the identical Avro/registry path — which is why demos exercise
the real pipeline, not a mock).

**Reference data:** `station_feeder_routes` in TimescaleDB — 409 rows mapping 70 subway
stations to the surface routes with a stop within 300m, with the distance. **Where it
came from matters for interviews:** `gtfs-loader` derives it from the City of Toronto's
*merged* GTFS feed (all modes) — identify subway stations (§4.3's saga), haversine
every surface stop against every station with a bounding-box prefilter, keep pairs
≤300m, record the minimum distance per (station, route). Derived, not hand-curated:
when the TTC opens a station or reroutes a bus, `make load-gtfs` regenerates the
mapping from coordinates. Sanity anchor: Bloor-Yonge → routes 97/300/320 (correct —
it's a subway-subway interchange with few surface connections); most-fed station:
Kennedy with 25 routes (correct — it's a massive bus terminal).

**Produces:** `ripple-alerts` — plain JSON keyed by `ripple_id`, also
`cleanup.policy=compact` (latest state per ripple retained; superseded versions
eventually cleaned). Same Avro-in/JSON-out boundary logic as docs 02/03: single
consumer, leaf topic, browser-bound payload.

**Downstream:** the API gateway writes each ripple to Redis (`ripple:{ripple_id}`,
**30-minute TTL** — the expiry stopgap of §4.8) and broadcasts `ripple_update` over
WebSocket; the dashboard's alert feed renders ripples indented under a `⤷ RIPPLE`
badge with "cascaded from {alert_id}."

**If it stops:** the alert feed keeps showing original TTC alerts (those flow through
the gateway directly); only the cascade rows age out over ≤30 minutes. Nothing else
depends on ripples. **If it's *wrong***: bad geometry in `station_feeder_routes` (say,
a 3km radius) would name dozens of irrelevant routes per alert — the output is
human-read, so wrongness here costs *credibility*, not corruption. That asymmetry (a
human in the loop as the final validator) is part of why the crude-but-inspectable
geospatial derivation was acceptable.

---

## 4. Bugs and decisions from actual project history

### 4.1 KTable-by-design, and the duplicate that proved the point

- **Decision first, vindication later.** Consuming `service-alerts` as a KTable was
  specified *before* topic compaction was configured, precisely because compaction
  couldn't be trusted to have run: the KTable gives upsert-by-key semantics **at read
  time**, independent of broker housekeeping.
- **The vindication, observed live.** During the compaction verification, the same
  `alert_id` (`DEMO-COMPACT-TEST`) was injected twice with different content
  ("Version ONE" / "Version TWO"). Reading the raw topic showed **both records still
  on the log** — compaction is performed by a background log-cleaner thread that only
  processes *rolled* (closed) segments, never the active one being written, so
  duplicates sit there for minutes-to-hours. The ripple detector, mid-test, stayed
  correct the whole time: its table simply held Version TWO as the current row.
- **Root cause of the potential trap (that we didn't fall into):** treating
  `cleanup.policy=compact` as if it guaranteed at-most-one-record-per-key on read.
  It guarantees that *eventually, for old segments*. A `stream()`-based consumer, or a
  naive `KafkaConsumer` assuming distinct keys, inherits the broker's cleaning schedule
  as a correctness dependency.
- **Lesson.** *Design consumers to be correct regardless of when a background process
  happens to run.* Compaction, GC, cache eviction, index vacuuming — background
  amortization is a storage optimization, never a semantic contract. If your logic
  needs latest-per-key, implement latest-per-key at read time (KTable, or an upsert
  into your own store) and let compaction merely keep the log small.

### 4.2 Understanding what compaction actually is (and why keys were mandatory)

- **What `cleanup.policy=compact` does.** Instead of deleting by age (the default
  `delete` policy), the log cleaner scans closed segments and, for each *key*, discards
  all but the most recent record. The topic converges toward a snapshot: one current
  value per key, retained indefinitely — a durable table you can rebuild state from at
  any time (this is exactly what the gateway exploits: on restart it replays
  `service-alerts` from offset 0 with a fresh consumer group to rebuild Redis).
- **Why it requires a message key.** Compaction's unit of "supersedes" is the key —
  with null keys there is no identity to collapse on (the cleaner has nothing to group
  by; unkeyed records on a compacted topic are a misconfiguration). Hence the checks
  that both producers set keys: the Go poller keys alerts by `alert_id`
  (`Key: []byte(alertID)`), the injector sets `key=alert["alert_id"].encode()`, and
  this service keys its output by `ripple_id`. The *Avro value* containing an
  `alert_id` field is irrelevant to the broker — compaction reads the record key
  bytes, not your payload schema.
- **Lesson.** *Compaction turns "topic" into "table," and the key is the primary key.*
  Choosing keys is schema design for the broker layer; get it wrong (or omit it) and
  retention semantics silently break.

### 4.3 The `location_type=1` overmatch: 114 "subway stations," 44 of them buses

- **Symptom.** Building the feeder table required "all subway stations." The GTFS spec
  says stations are `stops.txt` rows with `location_type=1`. Filtering the merged TTC
  feed on that yielded **114** rows — but eyeballing the names immediately smelled
  wrong: "Westmore," "Tobermory," "Signet Arrow" are not subway stations, they're bus
  loops and terminal platforms' parent nodes.
- **Investigation.** Cross-checked against ground truth (the TTC has ~70 subway
  stations across Lines 1/2/4). Then tested the join-based definition: routes with
  `route_type=1` (subway) → their trips → their stop_times → those platform stops'
  `parent_station` values. That set had exactly **70 members**, all with station names
  a Torontonian would recognize, Bloor-Yonge included.
- **Root cause.** The spec field is *semantically broader than assumed*: `location_type=1`
  means "a parent station structure," and the TTC models major bus terminals with
  parent stations too — legitimately, per spec. The overmatch was in the assumption
  "station ⇒ subway," not in the data.
- **Fix.** Define subway stations *behaviorally*: a station is a subway station iff
  subway service (route_type=1) actually stops at its child platforms. Structure fields
  tell you what something *is shaped like*; service data tells you what it *does*.
- **Lesson.** *Validate a filter against actual data — with a known-cardinality sanity
  check — before trusting a spec field's semantics.* "How many should there be?" (~70)
  is the cheapest possible test and it caught a 63% overmatch that would have polluted
  the feeder table with bus-terminal cascades. Same family as doc 03's direction_id
  lesson: the spec describes a universe of possible feeds; your feed is one specific
  citizen of it.

### 4.4 The 300-meter radius: derived geometry over a hand-built table

- **The alternative not chosen.** A hardcoded station→feeder-routes mapping (70 rows of
  local knowledge) would be *more accurate today* — a human knows route 97 serves
  Bloor-Yonge's front door. It was rejected because it rots invisibly: every TTC
  service change silently invalidates rows, and nothing would ever flag it.
- **What was built.** For each of the 70 stations, find every surface stop within
  **300m haversine distance** and collect those stops' routes with minimum distance.
  *Haversine* = great-circle distance between two lat/lon points on a sphere — the
  standard "distance between coordinates" formula (Earth-radius × angular distance;
  accurate to ~0.5% which is noise at 300m scale). A cheap bounding-box prefilter
  (skip stops whose latitude differs by more than ~330m-worth of degrees) avoids
  running the trig against all ~9,000 stops × 70 stations.
- **Why 300m, and why it's a commented tunable.** ~3–4 minutes' walk — a plausible
  "riders will actually transfer to this" threshold. It is a guess, and the code says
  so: the constant carries a comment noting that big interchange terminals (Kennedy's
  bus bays sprawl) may warrant more. Empirically it produced 409 relationships with
  believable extremes (Kennedy 25, Bloor-Yonge 3).
- **The honest limitation:** 68 of 70 stations got feeders; **2 got none** — their
  nearest surface stops sit just past 300m. For those stations, subway alerts cascade
  to nothing, silently (well — WARN-ly). A per-station radius or nearest-N fallback
  would fix it; nobody has needed it yet.
- **Lesson.** *Prefer derivable-from-source-of-truth over hand-maintained, even at some
  accuracy cost — and when a magic number is a guess, label it as a guess where the
  next engineer will trip over it.*

### 4.5 Deterministic ripple_id = idempotent reprocessing

- **The design.** Output key = `<alert_id>-<feeder_route_id>`. Not a UUID, not a
  timestamp-suffixed id: the *same logical fact always produces the same key*.
- **What "idempotent" means concretely here.** Processing alert X's update three times
  (KTable update, app restart replay, upstream re-publish after the ingestion service
  reboots and its dedup map resets — all real occurrences in this project's history)
  emits `X-97`, `X-300`, `X-320` three times — and every consumer treats that as three
  *overwrites of the same three things*: the compacted output topic retains one record
  per key; the gateway's Redis write to `ripple:X-97` replaces itself; the dashboard's
  client map keyed by ripple_id re-renders the same row. State everywhere converges to
  the same 3 ripples whether the computation ran once or thirty times. With UUID keys,
  every reprocess would *append* three new phantom ripples, and every layer downstream
  would need its own dedup logic.
- **Lesson.** *In at-least-once systems, deterministic keys are how you buy effective
  exactly-once on the cheap: make redelivery indistinguishable from delivery.* The
  question to ask of any derived event: "if this gets computed twice, what collides?"
  — and to *choose* the collision.

### 4.6 Join semantics: three decisions inside eight lines

Covered in Block 4; recorded here as decisions. (a) **Join on stop ids, not route
ids:** the alert's `affected_route_ids` says which *lines* are disrupted, but the
blast radius is spatial — it's the *stations* (stop ids) that determine which surface
routes absorb riders. (b) **Dedupe per feeder route to the closest affected station**,
otherwise multi-station alerts emit key collisions with racing payloads. (c) **Exclude
the alert's own routes** from its cascade — an alert must not list itself as its own
downstream victim. Each rule exists because the demo alert (Line 1,
Bloor-Yonge + St George) exercised the exact case: 5 matched rows → dedupe → 4 ripples
→ none of them route "1". **Lesson:** *joins over denormalized real-world data always
need explicit dedup and self-reference rules — write a concrete example first and
count what should come out.*

### 4.7 WARN-not-silent, and the time that convention paid for itself

- **The behavior.** Every alert that matches no station logs
  `alert X matched no stations ... no ripples emitted` at WARN, including the payload's
  stop ids. On the live feed that's *most alerts* — so during backlog replay the log is
  a wall of WARNs that are all expected.
- **Why that noise is accepted.** The chain of precedent: doc 02's missing-headway drop
  logged at `debug` (invisible, cost debugging time) → doc 03's spec demanded WARN →
  by this service, "expected zero-output paths must be visible" was house style. The
  payoff came *one service later*: in the API gateway, Spring silently passed whole
  `ConsumerRecord` envelopes instead of payloads to listener methods, and the guard
  clause — written WARN-not-silent per this same convention — printed
  `unexpected value type org.apache.kafka.clients.consumer.ConsumerRecord`, converting
  a would-be hours-long "why is Redis empty" mystery into a one-line diagnosis.
- **Lesson.** *A code path that intentionally produces nothing must say so — "no output"
  and "broken" are indistinguishable from outside, and the log line is what separates
  them.* Expected-noise WARNs are a tax; pay it, or spend the same hours during an
  outage instead. (Refinement for real production: a counter metric + sampled logs
  gives the visibility without the wall of text.)

### 4.8 The deferred problem: ripples never retract

- **The gap, stated honestly.** When the TTC resolves an alert, the ingestion service
  currently does nothing (it only publishes new/changed alerts; disappearance from the
  feed is not yet detected). So no tombstone reaches `service-alerts`, the KTable row
  lives forever, and — even if a tombstone *did* arrive — Block 2 returns an empty list
  rather than retracting previously-emitted ripples. Twice deferred.
- **The stopgap.** The gateway gives `ripple:{id}` Redis keys a **30-minute TTL**, so
  the *dashboard* forgets ripples on its own schedule even though the Kafka layer
  never does. Working consequence observed in practice: during a long pause in a demo
  session, injected ripples visibly evaporated from the UI while remaining on the
  topic — initially mistaken for a bug, actually the stopgap working exactly as
  designed.
- **What the real fix looks like (know this for the "how would you productionize"
  question):** (1) ingestion tracks the previous poll's alert-id set and publishes a
  *tombstone* (null value, keyed by alert_id) for each id that disappeared;
  (2) this service, on `alert == null`, looks up which ripple_ids it previously emitted
  for that alert — either from the deterministic key recipe re-applied to the *prior*
  row value (available: KTable updates can expose old value via `toStream` of a
  `TableJoined`… in practice, simplest is querying its own materialized state or
  recomputing from the tombstoned alert's last value) — and emits null-valued records
  for each `ripple_id`; (3) compaction then eventually physically removes both, and the
  gateway deletes `ripple:*` keys on tombstone instead of waiting out a TTL.
- **Lesson.** *Lifecycle has two halves; creating derived state is the easy half.* Every
  "X generates Y" design owes an answer to "what deletes Y?" — and "a TTL somewhere
  downstream" is a legitimate v1 answer *only if it's written down as debt*, which is
  what this section is.

---

## 5. Concepts glossary (delta over docs 02–03)

**KTable vs KStream / table-stream duality.** The same Kafka topic can be read as a
stream (every record is an independent event — how docs 02/03 read vehicle positions)
or as a table (records are upserts to keyed rows; only the latest per key is current —
how this service reads alerts). The duality: a table *is* the accumulation of its
change-stream; a change-stream *is* what you get by watching a table. `builder.table()`
materializes the topic into a local RocksDB store and emits row-changes;
`.toStream()` hands you those changes. Pick by question: "what happened?" → stream;
"what is true now?" → table.

**Log compaction.** The `cleanup.policy=compact` retention mode: instead of deleting
old segments wholesale by age, the broker's log-cleaner thread rewrites *closed*
segments keeping only each key's newest record. The topic converges to
one-current-value-per-key, retained forever — a rebuildable table (the gateway rebuilds
Redis from it on every boot). Two sharp edges used pointedly in this project: it needs
message keys to define identity, and it is *eventual* — the active segment is never
cleaned, so readers must tolerate duplicates (§4.1).

**Tombstone.** A record with a key and a **null value** — the deletion marker of the
keyed-topic world. Compaction treats it as "remove this key" (after a grace period,
the tombstone itself is also purged); a KTable treats it as row deletion and delivers
it to `toStream()` as `(key, null)`. This service handles receiving them (Block 2's
null check) but does not yet *emit* them — which is precisely the shape of the ripple
expiry gap (§4.8).

**Idempotency / deterministic keys.** An operation is idempotent when doing it twice
equals doing it once. In event pipelines you rarely get to *prevent* redelivery
(at-least-once is the default physics), so you engineer for it: derive output keys
deterministically from input identity (`alert_id + route_id`), make every downstream
write a keyed overwrite, and duplicates become harmless self-overwrites. The one-liner:
*make redelivery indistinguishable from delivery.*

**Stream-table join / enrichment join.** The general pattern this service instantiates:
a flow of triggering records is enriched against slower-moving keyed reference data.
Kafka Streams' native forms are KStream⋈KTable and KStream⋈GlobalKTable (co-partitioned
or replicated, respectively, both broker-fed). This service does the *pragmatic
off-framework variant* — the reference table lives in Postgres and is joined by SQL
per update — acceptable at ~alerts-per-minute volume, and the same
canonical-vs-pragmatic tradeoff discussed for the trips map in doc 03 §Block 2 (there:
in-memory bulk load; here: per-update query; the difference is input rate, 110/sec vs
a few/minute).

**Reference data, derived.** `station_feeder_routes` is reference data like doc 03's
trips map, with one upgrade in kind: it's *computed* from upstream source-of-truth
(GTFS coordinates) rather than transcribed. Regeneration replaces maintenance —
`make load-gtfs` after a schedule change refreshes the geometry, and drift becomes
impossible rather than invisible. The costs: derivation bugs (§4.3) replace
transcription bugs, and thresholds (300m) encode judgment that a hand-built table would
encode per-row.

**Haversine distance.** The great-circle ("as the crow flies") distance between two
latitude/longitude points, computed from the spherical law of haversines — the standard
formula whenever you have GPS coordinates and need meters. Used offline in the loader
(with a bounding-box prefilter so the trig runs on candidates, not the whole city).
Its known blind spot for this use: crow-flies ≠ walking distance — a station across a
rail corridor from a stop may be 250m away and a 900m walk.

**Eventual vs immediate consistency (compaction as the concrete case).** An eventually
consistent mechanism guarantees convergence to the desired state, not any deadline for
it. Compaction is the perfect teaching example because the inconsistency window is
plainly visible: write the same key twice, read the topic, see both versions sit there
until segment-roll + cleaner-run. Systems built on such mechanisms need read-time
semantics that are correct *during* the window (the KTable here; read-repair and
last-write-wins registers in the wider world) — treating "eventually X" as "X" is the
root of a whole genus of production bugs.
