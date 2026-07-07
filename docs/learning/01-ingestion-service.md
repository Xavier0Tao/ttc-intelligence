# 01 — Ingestion Service (Go)

> Study notes for `ingestion-service/`. Written for someone with solid Java/Spring
> experience but little Go, Docker, or Kafka background. Code quotes are from the
> actual source as of the session-5 fixes. Honesty over polish: several things in
> here were bugs we shipped, and a few things are still rough.

---

## 1. Purpose

The ingestion service is the single entry point for live data into the platform: it
polls the TTC's two public GTFS-Realtime feeds (vehicle positions every 15s, service
alerts every 30s) over plain HTTPS, converts each protobuf entity into an Avro record,
and publishes it to Kafka (`vehicle-positions`, `service-alerts`). It exists so that
every other service can be a *Kafka consumer* with no knowledge of the TTC's API, its
polling cadence, its protobuf quirks, or its outages — the "edge" messiness is
quarantined in one small process, and everything downstream sees a clean, schema-governed
stream.

---

## 2. Line-by-line walkthrough

The whole service is one file, `main.go` (~445 lines), plus `go.mod` and a `Dockerfile`.
That's a deliberate size choice: it does exactly one job (poll → convert → publish), and
splitting it into packages would add navigation cost without adding structure.

### Block 1: package and imports

```go
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)
```

**What it does.** `package main` + a `main()` function is Go's equivalent of a class with
`public static void main` — it marks this as an executable, not a library. The import
block is split into two groups by convention: standard library first, third-party second.

**The four dependencies, and why each one:**

- `gtfs-realtime-bindings` — Google publishes GTFS-Realtime as a `.proto` schema;
  MobilityData maintains generated Go classes for it. This is like using a published
  Maven artifact of JAXB/protobuf-generated classes rather than hand-parsing bytes.
- `goavro` — Avro encoder. We build Avro binary *manually* (see Block 12) instead of
  using Confluent's serializer, because Confluent doesn't ship a Go serializer with the
  same convenience as the Java one.
- `segmentio/kafka-go` — a **pure-Go** Kafka client. The obvious choice
  (`confluent-kafka-go`) wraps a C library and needs a C compiler at build time; this one
  doesn't. That distinction caused a real migration (see §4.1).
- `google.golang.org/protobuf` — the protobuf runtime, used for one call:
  `proto.Unmarshal` on the raw feed bytes.

**Go note (visibility).** Go has no `public`/`private` keywords. A name that starts with
a capital letter (`kafka.Writer`, `proto.Unmarshal`) is exported (public); lowercase
(`pollVehicles`, `getEnv`) is package-private. Every function in this file is lowercase
because nothing imports this package.

### Block 2: constants

```go
const (
	vehiclesFeedURL     = "https://bustime.ttc.ca/gtfsrt/vehicles"
	alertsFeedURL       = "https://bustime.ttc.ca/gtfsrt/alerts"
	vehiclePollInterval = 15 * time.Second
	alertPollInterval   = 30 * time.Second
	vehiclesTopic       = "vehicle-positions"
	alertsTopic         = "service-alerts"
	vehiclesSubject     = "vehicle-positions-value"
	alertsSubject       = "service-alerts-value"
)
```

**What it does.** Feed URLs, poll cadences, topic names, and Schema Registry *subject*
names. A subject is the registry's namespace for a schema's version history; the
`-value` suffix is the Confluent convention meaning "this is the schema for the message
*value* of this topic" (keys can have their own subject).

**Why 15s/30s.** The TTC feed itself updates roughly every 15–20 seconds, so polling
faster buys nothing. Alerts change far less often, so 30s is generous.

**Honest rough edge.** The URLs and intervals are compile-time constants, not env vars.
Broker address and registry URL *are* configurable (they differ between local runs and
Docker), but nobody has needed to point this at a different transit agency, so the URLs
stayed hardcoded. An interviewer might reasonably push on this: the "right" answer is
that config should be env-driven when it varies across deployments, and these don't (yet).

**Go note (`time.Duration`).** `15 * time.Second` is not "15 seconds as an int" — Go has
a typed `Duration` (an `int64` of nanoseconds under the hood), so you can't accidentally
pass seconds where milliseconds are expected, a classic Java `Thread.sleep(15)` bug.

### Block 3: `main()` — configuration, schemas, writers, and the two pollers

```go
func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	schemasDir := getEnv("SCHEMAS_DIR", filepath.Join("..", "schemas"))

	vehicleCodec, vehicleSchemaID := mustPrepareSchema(schemaRegistryURL, schemasDir, "vehicle_position.avsc", vehiclesSubject)
	alertCodec, alertSchemaID := mustPrepareSchema(schemaRegistryURL, schemasDir, "service_alert.avsc", alertsSubject)

	vehicleWriter := newWriter(broker, vehiclesTopic)
	defer vehicleWriter.Close()
	alertWriter := newWriter(broker, alertsTopic)
	defer alertWriter.Close()

	httpClient := &http.Client{Timeout: 10 * time.Second}

	log.Printf("ingestion-service started: ...")

	var wg sync.WaitGroup
	wg.Add(2)
	go pollVehicles(&wg, httpClient, vehicleWriter, vehicleCodec, vehicleSchemaID)
	go pollAlerts(&wg, httpClient, alertWriter, alertCodec, alertSchemaID)
	wg.Wait()
}
```

**What it does, in order:**

1. Reads three env vars with defaults chosen for *local development* (`localhost:9092`,
   `../schemas`). The Docker image overrides them (`kafka:29092`, `/app/schemas`) via
   `ENV` lines in the Dockerfile. One binary, two environments, zero code changes.
2. Loads each Avro schema from disk, registers it with Schema Registry, and builds a
   codec — failing fast (process exit) if any of that fails, because the service is
   useless without its schemas.
3. Creates one Kafka writer per topic.
4. Builds a shared HTTP client with a 10s timeout. **Why the timeout matters:** Go's
   default `http.Client` has *no* timeout. If the TTC endpoint ever black-holed a
   connection, a naive client would hang a poller forever, silently. (Java's
   `HttpClient` has the same trap — no default request timeout.)
5. Starts the two pollers as goroutines and blocks forever.

**Why one writer per topic instead of one shared writer?** `kafka.Writer` can be created
without a fixed topic (topic per message), but binding topic-per-writer keeps each
poller's failure and batching behavior independent, and makes it impossible to publish a
vehicle event to the alerts topic by passing the wrong argument.

**Go note (goroutines vs threads).** `go pollVehicles(...)` starts a *goroutine* — a
function running concurrently, scheduled by the Go runtime onto a small pool of OS
threads. They cost ~a few KB of stack each (vs ~1MB for a Java platform thread), so
"just start one per concurrent task" is idiomatic rather than something you'd pool.
Java's closest modern analog is virtual threads (Loom); before that, you'd reach for an
`ExecutorService`.

**Go note (`sync.WaitGroup`).** Functionally a `CountDownLatch`: `wg.Add(2)` sets the
count, each goroutine calls `wg.Done()` when it returns, `wg.Wait()` blocks until zero.
Here the pollers loop forever, so `Wait()` never returns — it's the idiom for "keep
`main` alive," because **a Go program exits when `main` returns, even if goroutines are
still running** (unlike Java, where any non-daemon thread keeps the JVM alive). If we
skipped `wg.Wait()`, the process would start two pollers and immediately exit.

**Go note (`defer`).** `defer vehicleWriter.Close()` schedules the call for when the
surrounding function returns — like `finally`, but declared next to the acquisition
instead of at the bottom. Deferred calls run last-in-first-out. Honest caveat: since
`main` never returns (the `Wait()` blocks forever) these particular defers never fire in
practice; there's no signal handling / graceful shutdown in this service. Docker just
kills it. That's a real rough edge, acceptable because the service is stateless and
Kafka acks the batches synchronously — the worst case on kill is re-publishing a poll's
worth of data, which downstream handles (vehicles are keyed snapshots, alerts are
deduped/compacted).

**Go note (`:=`).** `broker := getEnv(...)` declares *and* assigns with inferred type —
Java's `var`, but used pervasively.

### Block 4: schema loading and registration

```go
func mustPrepareSchema(registryURL, schemasDir, filename, subject string) (*goavro.Codec, int) {
	schema, err := loadSchema(schemasDir, filename)
	if err != nil {
		log.Fatalf("failed to load schema: %v", err)
	}
	id, err := registerSchema(registryURL, subject, schema)
	...
	codec, err := goavro.NewCodec(schema)
	...
	return codec, id
}
```

and the interesting part, `registerSchema`:

```go
	url := fmt.Sprintf("%s/subjects/%s/versions", registryURL, subject)
	...
	var lastErr error
	for attempt := 1; attempt <= 10; attempt++ {
		resp, err := client.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewReader(body))
		...
		log.Printf("schema registration attempt %d/10 for %s failed: %v (retrying in 3s)", ...)
		time.Sleep(3 * time.Second)
	}
	return 0, lastErr
```

**What it does.** POSTs the schema JSON to Schema Registry's REST API. The registry
responds with a numeric **schema id** which we embed in every message (Block 12).
Registration is *idempotent*: posting an identical schema returns the existing id;
posting a backward-compatible evolution creates version 2 of the same subject (this is
exactly what happened when we added `trip_id`/`direction_id`/`current_stop_sequence` —
the subject went to `[1,2]` live, no downtime).

**Why the retry loop.** In Docker Compose, "depends_on: healthy" ordering mostly saves
us, but for local `make ingest` runs the registry might still be booting. Ten attempts ×
3s is a crude but effective "wait for dependency" — the alternative (crash and let the
user retry) is strictly worse for a dev loop, and the alternative alternative
(exponential backoff library) is overkill for two calls at startup.

**Why `must...` + `log.Fatalf`.** The `mustX` naming convention in Go signals "this
panics/exits on failure instead of returning an error." At startup that's the right
call: there is no degraded mode worth running in if the schema can't be loaded or
registered. `log.Fatalf` prints and calls `os.Exit(1)` — in Docker, the container dies
and `restart: unless-stopped` retries it, which is the supervision loop doing its job.

**Go note (errors as values).** Go has no exceptions for normal control flow. Functions
return `(result, error)` and you check `if err != nil` explicitly at every call site.
Compared to Java's exceptions it's more verbose but makes the failure paths visible in
the code — you can *see* which calls this service considers fatal (`log.Fatalf`) versus
survivable (log and continue to the next poll). The `%w` verb in
`fmt.Errorf("reading %s: %w", ..., err)` *wraps* the underlying error, preserving the
cause chain like exception chaining.

### Block 5: the Kafka writer

```go
func newWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
}
```

**What it does.** Configures a producer for one topic. `Balancer` decides which
*partition* a message goes to **when it has no key** — but every message we send has a
key (vehicle id / alert id), and keyed messages are hashed to a partition so that all
events for the same key land in the same partition, in order. That ordering guarantee is
what lets downstream consumers treat the stream per-vehicle/per-alert as sequential.

**`AllowAutoTopicCreation: true`** is a dev-loop convenience (first local run before
`kafka-init` created topics). In production you'd turn this off — auto-created topics
get default partition/replication settings and, worse, default `cleanup.policy` — our
`service-alerts` topic *needs* to be compacted, which only the explicit
`kafka-init` creation does correctly. Honest assessment: this flag is a leftover
convenience that could mask a misconfigured environment.

**Go note (struct literals, no constructors).** Go has no constructors or builders; you
create configured objects with struct literal syntax, and any field you don't mention
gets its zero value. The zero values here are load-bearing: unset `BatchTimeout`
defaults to **1 second**, which caused the worst bug in this service's history (§4.6).

### Block 6: the poller loop pattern

```go
func pollVehicles(wg *sync.WaitGroup, client *http.Client, writer *kafka.Writer, codec *goavro.Codec, schemaID int) {
	defer wg.Done()

	ticker := time.NewTicker(vehiclePollInterval)
	defer ticker.Stop()

	pollAndPublishVehicles(client, writer, codec, schemaID)
	for range ticker.C {
		pollAndPublishVehicles(client, writer, codec, schemaID)
	}
}
```

**What it does.** Fires one poll immediately (so startup doesn't wait 15 silent
seconds), then re-polls on every tick, forever.

**Go note (channels and `for range`).** `ticker.C` is a *channel* — Go's built-in typed,
blocking queue, the primitive goroutines use to communicate. `for range ticker.C` reads
from the channel in a loop, blocking until the next tick arrives. It's the idiomatic
"scheduled task" — compare Java's `ScheduledExecutorService.scheduleAtFixedRate`.

**A subtle behavior worth knowing for interviews:** `time.Ticker` does not queue up
missed ticks (its channel has a buffer of 1). If a poll takes longer than the interval,
you don't get a burst of catch-up polls afterward — you just skip beats. During the
throughput bug (§4.6) a "poll" effectively took ~30 minutes, and this property meant the
service degraded to back-to-back slow polls rather than an unbounded backlog. The
graceful degradation accidentally helped *hide* the bug.

### Block 7: publishing a vehicles poll as one batch

```go
	// All of a poll's events go out in ONE WriteMessages call: kafka-go's
	// Writer flushes per call (or per BatchTimeout, default 1s), so writing
	// message-by-message caps throughput at ~1 msg/s — a full TTC feed of
	// ~2000 vehicles would take half an hour per "poll".
	messages := make([]kafka.Message, 0, len(feed.GetEntity()))
	for _, entity := range feed.GetEntity() {
		vehicle := entity.GetVehicle()
		if vehicle == nil {
			continue
		}

		vehicleID, native := vehicleToAvroNative(vehicle)
		payload, err := encodeConfluentAvro(codec, schemaID, native)
		if err != nil {
			log.Printf("error encoding vehicle event: %v", err)
			continue
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(vehicleID),
			Value: payload,
		})
	}

	if err := produceBatch(writer, messages); err != nil {
		log.Printf("error producing vehicle batch: %v", err)
		return
	}
	log.Printf("Vehicles poll: %d positions published", len(messages))
```

**What it does.** Builds the full slice of messages for this poll, then hands them to
the writer in a single call. The comment at the top is a scar, not documentation — this
loop used to call the writer once per message, and that version shipped and "worked"
for three sessions (§4.6 tells that story properly).

**Why per-message errors `continue` instead of failing the poll.** One malformed entity
(it happens — feeds contain half-populated vehicles) shouldn't discard the other 1,900.
Encode errors are logged and skipped; only the *batch write* failing aborts the poll,
and even then the next tick retries fresh. This is the "at-least-once, favor
availability" stance: the worst case is a vehicle position arriving 15s later, which is
self-healing because every poll republishes current state anyway.

**Why `Key: vehicleID`.** Three reasons: (1) per-vehicle ordering within a partition, as
above; (2) downstream, the API gateway keys Redis entries by vehicle id — a stable key
means updates overwrite instead of duplicate; (3) if we ever compact this topic, the key
is what compaction retains the latest record per.

**Go note (slices and `make` with capacity).** `make([]kafka.Message, 0, n)` creates a
slice with length 0 but *capacity* n — like `new ArrayList<>(n)`, it pre-reserves so
`append` doesn't repeatedly reallocate. `append` returns a (possibly new) slice, hence
`messages = append(messages, ...)` — a Go-ism that trips up everyone once.

### Block 8: protobuf → Avro conversion, and the optionality dance

```go
	native := map[string]interface{}{
		"vehicle_id":            vehicleID,
		"route_id":              routeID,
		...
		"bearing":               nil,
		"speed":                 nil,
		...
	}

	if pos := v.GetPosition(); pos != nil {
		native["latitude"] = float64(pos.GetLatitude())
		native["longitude"] = float64(pos.GetLongitude())
		if pos.Bearing != nil {
			native["bearing"] = goavro.Union("float", pos.GetBearing())
		}
		...
	}
	if v.CurrentStopSequence != nil {
		native["current_stop_sequence"] = goavro.Union("int", int32(v.GetCurrentStopSequence()))
	}
```

**What it does.** goavro encodes from a `map[string]interface{}` ("native" form) rather
than from typed structs. Fields that are optional in the Avro schema (declared as
`["null", "float"]` unions) must be set to either `nil` or a goavro *union wrapper*
`goavro.Union("float", value)` — the wrapper tells the encoder which branch of the union
this value takes.

**Why the two-step nil checks (`pos.Bearing != nil` then `pos.GetBearing()`).** Protobuf
optional fields in Go are pointers: `Bearing *float32`. A nil pointer means "the feed
didn't send this field," which is semantically different from "the feed sent 0.0" — a
bearing of 0 is due north! The generated `GetBearing()` getter is nil-safe (returns the
zero value on nil), so we check the raw pointer for *presence* and only then read
through the getter. Java's protobuf bindings expose the same idea as `hasBearing()`.
Collapsing this to just `GetBearing()` would silently convert "unknown" into "north at
0 m/s", which downstream would render as a real heading.

**Real-world payoff of preserving optionality:** the TTC feed *never* populates
`direction_id`. Because we pass `nil` through honestly instead of defaulting to 0,
downstream (the crowding estimator) could detect "missing" and resolve direction from
static GTFS instead of trusting a fabricated 0 (which would have been silently wrong
for half the vehicles — direction 1 trips would all be mislabeled).

**Go note (`interface{}`).** `interface{}` (spelled `any` in modern Go) is the empty
interface — every type satisfies it, so it's Go's `Object`. Using a
`map[string]interface{}` here trades compile-time safety for not needing generated Avro
classes; the cost is that a typo'd field name fails at *encode time*, not compile time.
The Java services in this repo made the opposite trade (avro-maven-plugin codegen).

### Block 9: the alerts poller and its dedup state

```go
func pollAlerts(...) {
	defer wg.Done()

	// alert_id -> content fingerprint of the last published version. GTFS-RT
	// alerts carry no per-alert timestamp (the feed header timestamp changes
	// on every poll), so "changed" is detected by comparing alert content.
	published := make(map[string]string)
	...
}
```

**What it does.** Alerts, unlike vehicle positions, are *stateful* objects that persist
across polls — the same 40 alerts come back every 30 seconds. Publishing all of them
every poll would flood the topic with duplicates, so the poller remembers a
**fingerprint** of each alert it has published and only re-publishes on change.

**Why the map lives inside `pollAlerts` and needs no mutex.** It's owned by exactly one
goroutine. Go's concurrency slogan is "share memory by communicating" — but the simpler
version used here is *don't share at all*. If both pollers touched this map we'd need a
`sync.Mutex` (maps in Go are not thread-safe and concurrent writes are a runtime crash,
not just a race).

**Honest rough edges:** (1) the map grows forever — alert ids that the TTC resolves are
never evicted. At ~tens of alerts/day this leaks bytes per day, irrelevant in practice,
but it's the textbook "unbounded cache" smell an interviewer might probe. (2) The state
is in-memory, so a restart republishes every active alert once. That's harmless *by
design elsewhere*: the topic is compacted and keyed by `alert_id`, and consumers treat
messages as upserts — a deliberate example of making the pipeline idempotent instead of
making the producer perfect.

### Block 10: fingerprint dedup, and publishing alerts as a batch

```go
	if prev, seen := publishedAlerts[alertID]; seen && prev == fingerprint {
		continue
	}
	...
	// Single batch write per poll (see pollAndPublishVehicles for why);
	// dedup state only advances once the batch is actually accepted.
	if err := produceBatch(writer, messages); err != nil {
		log.Printf("error producing alert batch: %v", err)
		return
	}
	for _, a := range pending {
		publishedAlerts[a.id] = a.fingerprint
		...
	}
```

and the fingerprint itself:

```go
// alertFingerprint captures the fields that constitute a meaningful change to
// an alert. timestamp and active_since are deliberately excluded: both fall
// back to the feed-header timestamp (which moves every poll) when the alert
// carries no explicit values, and including them would defeat deduplication.
func alertFingerprint(native map[string]interface{}) string {
	routes := append([]string(nil), native["affected_route_ids"].([]string)...)
	stops := append([]string(nil), native["affected_stop_ids"].([]string)...)
	sort.Strings(routes)
	sort.Strings(stops)
	return fmt.Sprintf("%v|%v|%v|%v|%s|%s", ...)
}
```

**What it does.** The fingerprint is a canonical string of the alert's *meaningful*
fields (effect, cause, texts, sorted route/stop lists). Route and stop lists are sorted
so the same alert with entities in a different order doesn't look "changed."

**The comment is another scar.** The first version included `active_since` in the
fingerprint. `active_since` falls back to the feed-header timestamp when an alert has no
explicit active period — and the feed-header timestamp changes every poll. Result: all
37 live alerts were re-published every 30 seconds, and we only caught it by reading the
poll logs ("37 active, 37 new published" three polls in a row). §4.5 has the full story.

**Why dedup state advances only after the batch succeeds.** If we marked alerts as
published *before* the write and the write failed, those alerts would never be retried —
silent data loss. Ordering it write-then-commit gives at-least-once: a failure means the
next poll re-attempts. (The two-slice `messages`/`pending` construction exists exactly
to keep "what to send" and "what to remember on success" in sync.)

**Go note (comma-ok map lookup).** `prev, seen := publishedAlerts[alertID]` — map reads
return a second boolean telling you whether the key existed. Java's
`map.containsKey` + `map.get` in one step, without the "get returned null but null might
be a value" ambiguity.

### Block 11: small protobuf helpers

```go
func firstTranslation(ts *gtfs.TranslatedString) string {
	for _, t := range ts.GetTranslation() {
		if text := t.GetText(); text != "" {
			return text
		}
	}
	return ""
}
```

GTFS-RT wraps every human-readable string in a `TranslatedString` (a list of
per-language translations). The TTC only publishes English, so "first non-empty
translation" is a pragmatic simplification — a multi-language agency would need a
language parameter here. Note the nil-safety freebie: calling `GetTranslation()` on a
nil `*TranslatedString` returns an empty list rather than a NullPointerException — the
generated getters absorb the nil checks that Java code would need explicitly.

### Block 12: the Confluent wire format — the most interview-worthy 10 lines

```go
// encodeConfluentAvro serializes a native Avro value using the Confluent wire
// format: magic byte 0x00, 4-byte big-endian schema id, then Avro binary.
func encodeConfluentAvro(codec *goavro.Codec, schemaID int, native map[string]interface{}) ([]byte, error) {
	avroBytes, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 5, 5+len(avroBytes))
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	return append(buf, avroBytes...), nil
}
```

**What it does.** Avro binary alone is *not self-describing* — you cannot decode it
without knowing the exact schema it was written with. Confluent's convention solves this
by prefixing every message with 5 bytes: a magic `0x00`, then the registry-assigned
schema id, big-endian. A consumer reads the id, fetches (and caches) that schema from
the registry, and decodes. Every Confluent deserializer on the JVM side
(`KafkaAvroDeserializer`, `kafka-avro-console-consumer`) expects exactly this prefix.

**Why hand-roll it.** In Java you get this for free from `KafkaAvroSerializer`. The Go
ecosystem doesn't have an equally blessed equivalent, and the format is genuinely five
bytes — writing it manually was less risk than adopting a third-party wrapper library.
This is also the best possible demonstration that the "magic" in Confluent serializers
is small and knowable.

**Why this matters downstream:** this is the contract that lets the Java services
(`delay-predictor`, `crowding-estimator`, `api-gateway`) deserialize with stock
Confluent classes and *generated* Java types from the same `.avsc` files. Producer in
Go, consumers in Java, single source of schema truth — the wire format is the meeting
point.

**Go note (byte slices).** `make([]byte, 5, 5+len(avroBytes))` — length 5 (the header we
index into), capacity sized for the whole message so the final `append` copies once.
`binary.BigEndian.PutUint32` writes the int into bytes 1–4; network byte order because
the Confluent spec says so.

### Block 13: fetching and parsing the feed

```go
func fetchFeed(client *http.Client, url string) (*gtfs.FeedMessage, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	...
	feed := &gtfs.FeedMessage{}
	if err := proto.Unmarshal(body, feed); err != nil {
		return nil, err
	}
	return feed, nil
}
```

**What it does.** Plain GET, read the whole body (~a few hundred KB of protobuf),
`proto.Unmarshal` into the generated `FeedMessage` struct. `defer resp.Body.Close()`
right after the error check is *the* canonical Go HTTP pattern — forgetting it leaks
connections (Go reuses keep-alive connections only if bodies are drained and closed).

**Honest rough edge.** No check of `resp.StatusCode`. If the TTC returned a 500 with an
HTML error page, `proto.Unmarshal` would fail with a confusing "cannot parse" error
rather than a clear "server returned 500". It works because protobuf parsing *does* fail
loudly, but the error message quality is worse than it should be. Cheap improvement
nobody has made yet.

### Block 14: `go.mod` and the Dockerfile

```go
module ttc-intelligence/ingestion-service

go 1.22

require (
	github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs v1.0.0
	github.com/linkedin/goavro/v2 v2.15.0
	github.com/segmentio/kafka-go v0.4.47
	google.golang.org/protobuf v1.33.0
)
```

`go.mod` ≈ `pom.xml`, `go.sum` ≈ a lockfile with checksums (supply-chain tamper
detection). `go mod tidy` recomputes both from actual imports.

```dockerfile
# Build context is the monorepo root so the shared schemas/ directory is available.
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY ingestion-service/go.mod ingestion-service/go.sum* ./
RUN go mod download
COPY ingestion-service/ .
RUN CGO_ENABLED=0 GOOS=linux go build -o ingestion-service .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /app/ingestion-service .
COPY schemas /app/schemas
ENV KAFKA_BROKER=kafka:29092
ENV SCHEMAS_DIR=/app/schemas
CMD ["./ingestion-service"]
```

**Multi-stage build.** Stage 1 has the full Go toolchain (~800MB of image); stage 2 is
bare Alpine (~7MB) plus the compiled binary and the schema files. The final image ships
no compiler, no source, no package manager bloat. Java equivalent: building the jar in a
`maven` image and copying it into a `jre` image — same pattern, and this repo's Java
services do exactly that.

**Layer-caching trick.** `go.mod`/`go.sum` are copied and `go mod download` runs *before*
the source is copied. Docker caches layers by content: editing `main.go` invalidates
only the layers after `COPY ingestion-service/ .`, so dependency downloads are skipped
on every rebuild. Same trick as copying `pom.xml` + `dependency:go-offline` first.

**`CGO_ENABLED=0`** forces a fully static binary with no libc dependency — which is why
it can run on bare Alpine (musl libc) with zero compatibility concerns, and why the
image needs only `ca-certificates` (for HTTPS to the TTC) added.

**Why the build context is the repo root.** The image needs `schemas/`, which lives
*outside* `ingestion-service/`. Docker can only COPY from within the build context, so
compose sets `context: .` with `dockerfile: ingestion-service/Dockerfile`. This was a
consequence of the schema-unification decision (§4.4).

---

## 3. How this service connects to the rest of the system

**Reads from:** only the two public TTC endpoints. No database, no Redis, no other
service. It is the one component allowed to talk to the outside world.

**Writes to:**

| Topic | Cadence | Key | Format |
|---|---|---|---|
| `vehicle-positions` (3 partitions) | ~1,000–2,000 msgs per 15s poll | `vehicle_id` | Avro `VehiclePosition` (Confluent wire format) |
| `service-alerts` (1 partition, **compacted**) | only on change, ~40 active alerts | `alert_id` | Avro `ServiceAlert` (Confluent wire format) |

It also *writes to Schema Registry* at startup (schema registration) — meaning this
service, as the producer, is the one that drives schema evolution: when we added three
fields to `vehicle_position.avsc`, restarting ingestion is what created version 2 of the
subject.

**Who consumes what:**

- `delay-predictor` (Kafka Streams) ← `vehicle-positions`: windows vehicles per route,
  compares against scheduled headways → `delay-predictions`.
- `crowding-estimator` (Kafka Streams) ← `vehicle-positions`: uses
  `current_stop_sequence` + resolved direction → `crowding-estimates`.
- `ripple-detector` (Kafka Streams) ← `service-alerts` as a KTable: joins
  `affected_stop_ids` against subway feeder routes → `ripple-alerts`.
- `api-gateway` (Spring Boot) ← both topics (plus the three derived ones): materializes
  into Redis, broadcasts over WebSocket to the dashboard.

**If this service stops:** nothing crashes — that's the point of the broker in the
middle. But the system goes visibly stale in layers: the gateway's Redis vehicle keys
have a 120s TTL, so the dashboard map empties within two minutes; the delay/crowding
5-minute windows stop closing (stream time only advances with new records), so those
panels freeze at their last values; alerts stay (no TTL — the TTC "resolving" alerts is
future work) and ripples expire after their 30-minute TTL. Restarting ingestion
self-heals everything: the next poll republishes the full current state of the world,
and alerts get re-fingerprinted and deduped from scratch.

**Why Avro and not JSON for the output format:** (1) a *contract* — consumers get
compile-time generated classes and can't silently drift from the producer; (2) schema
*evolution rules* enforced by the registry — the v1→v2 field additions were checked for
backward compatibility before being accepted, so old messages remained readable by new
consumers mid-flight; (3) size — binary Avro with a 5-byte header beats repeating JSON
field names in every one of ~5M messages/day. The real cost we paid: binary messages are
not `kafka-console-consumer`-friendly (you need `kafka-avro-console-consumer` and the
registry up), and the first JSON→Avro migration left old-format messages on the topic
that made every downstream consumer need a "log and skip garbage" deserialization
handler. Both were worth it, neither was free.

---

## 4. Bugs and decisions from actual project history

Everything below actually happened in this repo's history, in roughly this order. The
lessons are the part worth rehearsing.

### 4.1 The CGO wall: `confluent-kafka-go` → `segmentio/kafka-go`

- **Symptom.** The original scaffold used `confluent-kafka-go`, Confluent's official Go
  client. It would not build on the dev machine: the library wraps `librdkafka` (a C
  library) and requires CGO — Go's bridge for calling C — which requires a C toolchain
  that the Windows dev machine didn't have.
- **Investigation.** Short: the build error says it. The decision was whether to install
  MinGW/a C compiler, build only in Docker, or switch libraries.
- **Root cause.** Dependency choice with a hidden native-code requirement, colliding
  with the actual dev environment.
- **Fix.** Swap to `segmentio/kafka-go`, a pure-Go client: rewrite the producer calls
  (`Producer.Produce` + delivery-report channel → `Writer.WriteMessages`), then delete
  `librdkafka` and `CGO_ENABLED=1` from the Dockerfile, shrinking it in the process.
- **Lesson.** *A dependency's build-time requirements are part of its API.* "Official"
  and "most popular" clients can lose to a plainer library that matches your build
  environment. Also: this swap later *caused* §4.6 — replacing a library means
  relearning its performance model, not just its method names.

### 4.2 Docker networking: `kafka:29092` vs `localhost:9092`

- **Symptom category.** A Kafka client inside a container that connects to
  `kafka:9092` appears to connect fine, then produces/consumes nothing (or times out on
  metadata).
- **Investigation.** Understanding Kafka's *advertised listeners*: on connection, the
  broker sends back metadata saying "here is the address you should talk to." If a
  container connects to a listener whose advertised address is `localhost:9092`, the
  client then tries `localhost` — which inside a container is *itself*, not the broker.
- **Root cause.** Kafka's two-step connection model (bootstrap → advertised address) ×
  Docker's per-container network namespaces. This is probably the single most common
  Kafka-in-Docker failure in existence.
- **Fix.** The broker is configured with two listeners: `PLAINTEXT://kafka:29092`
  (advertised for the Docker network) and `PLAINTEXT_HOST://localhost:9092` (advertised
  for the host). Containers get `KAFKA_BROKER=kafka:29092`; local `make ingest` runs
  default to `localhost:9092`. The ingestion service just reads the env var — the
  network topology knowledge lives in compose, not code.
- **Lesson.** *With Kafka, the address you dial is only a bootstrap; the address you
  USE is whatever the broker advertises.* Any "connects but no data flows" symptom in
  containerized Kafka should trigger a check of advertised listeners first.

### 4.3 The JSON → Avro / Schema Registry migration

- **What happened.** Sessions 1–2 published plain JSON. Session 2 migrated to Avro with
  Schema Registry: schema file, startup registration with retries, hand-rolled Confluent
  wire format (Block 12), goavro union wrappers for optional fields.
- **Why (the honest version).** Partly because typed contracts and evolution rules are
  genuinely the right call before multiple JVM consumers existed — and partly because
  this project targets streaming-infrastructure jobs and Schema Registry fluency was an
  explicit goal. Resume-driven development, honestly acknowledged, that then paid off
  technically: the v2 evolution (three new nullable fields) shipped against the *live*
  registry with zero downstream changes required.
- **The migration scar.** Old JSON messages remained on `vehicle-positions` after the
  switch. Kafka Streams consumers default to reading from the earliest offset, hit
  non-Avro bytes (no magic byte), and would crash-loop. Every Java consumer in the repo
  now sets `LogAndContinueExceptionHandler` for deserialization errors — a permanent
  fixture that exists because of this one migration.
- **Lesson.** *You cannot migrate a Kafka topic's format atomically — old bytes don't go
  away.* Either version the topic name, or make every consumer tolerant of the old
  format. And optional-with-default fields are what make schema evolution painless;
  required fields are forever.

### 4.4 Schema duplication → shared `schemas/` directory

- **Symptom.** `vehicle_position.avsc` existed in two places (`ingestion-service/avro/`
  and `delay-predictor/src/main/avro/`) that had to be byte-identical for producer and
  consumer to agree. Nothing enforced that.
- **Fix, and what it dragged along.** One `schemas/` directory at the repo root. But the
  Go side had been *embedding* the schema at compile time with `go:embed`, which cannot
  reference files outside the module directory. So embedding became runtime file loading
  (`SCHEMAS_DIR` env var, default `../schemas` for local runs), and every Dockerfile's
  build context moved to the repo root so `COPY schemas ...` works.
- **Lesson.** *Copies of a contract are a bug that hasn't fired yet.* Single-source it
  even when the mechanics are annoying. Corollary: build-time embedding trades runtime
  robustness (nothing to find on disk) for coupling to the build layout — the moment a
  file becomes shared, embedding fights you.

### 4.5 The alert dedup fingerprint that never matched

- **Symptom.** Logs showed `Alerts poll: 37 active, 37 new published` on *every* poll —
  dedup published everything, every 30 seconds, flooding the compacted topic with
  identical records.
- **Investigation.** The fingerprint was supposed to change only when content changed.
  Reading the field list against the mapping code: `active_since` falls back to the
  *feed-header timestamp* when the alert has no explicit active period — and most TTC
  alerts don't. The feed header timestamp changes every poll, therefore so did every
  fingerprint.
- **Root cause.** A "stable" identity function accidentally included a field derived
  from wall-clock time.
- **Fix.** Exclude `timestamp` *and* `active_since` from the fingerprint (both can be
  poll-time-derived); keep effect/cause/texts/sorted entity lists. Verified by watching
  three consecutive polls go `38 published → 0 → 1` (the 1 being a genuinely new alert).
- **Lesson.** *Dedup/cache keys must be built from fields with stable semantics, and
  every fallback value is part of the key's semantics.* Also: per-poll counters
  (`N active, M new`) are what made this visible — design logs so that the wrong
  behavior *looks* wrong.

### 4.6 The 1-message-per-second producer (the big one)

- **Symptom (as experienced).** The dashboard map showed only ~120 vehicles for a city
  that runs ~2,000, and users saw "a few streetcar icons." For *three sessions* nobody
  noticed, because every intermediate verification asked "does data flow?" — and it did.
- **Investigation.** While debugging an unrelated dashboard bug in a real browser, the
  vehicle count looked absurdly low. Working backward: Redis held exactly ~120
  `vehicle:*` keys with a 120s TTL → so only ~1 vehicle/second was being *written* → the
  ingestion logs, re-read with fresh eyes, showed exactly one `published vehicle
  position` line per second, all along.
- **Root cause.** `kafka.Writer.WriteMessages` is *synchronous*: it returns when the
  batch containing your messages is flushed. The writer's default `BatchTimeout` is
  **1 second**, and we called it once per message — so each call sat waiting for the 1s
  batch window. Throughput: ~1 msg/s. A ~2,000-entity poll took ~33 minutes; the ticker
  (Block 6) just dropped beats, so the service ran one endless slow "poll," publishing
  each vehicle roughly once per half hour. Redis's 120s TTL then kept only the ~120 most
  recent.
- **Fix.** Accumulate the poll's messages in a slice and make ONE `WriteMessages` call
  (Block 7). Result: 994 messages published instantly on the next poll, ~1,700 at rush
  hour, and the map filled with the real fleet.
- **Lessons (several, all transferable).**
  1. *Synchronous send + producer-side batching is a throughput trap.* Every batching
     producer (kafka-go, the Java producer's `linger.ms`, JDBC batch inserts) has this
     shape: per-item synchronous calls degenerate to one item per batch window.
  2. *"Data flows" is not verification; "data flows at the expected rate" is.* Every
     check for three sessions confirmed liveness, none confirmed volume. One
     back-of-envelope number (2,000 vehicles / 15s ≈ 130 msg/s expected) would have
     caught it on day one.
  3. *The logs showed the bug the whole time* — one line per second is a rhythm you can
     see in timestamps. Log *rates* are data, not just log *contents*.
  4. A bug can be masked by two others: the dashboard's own state bugs (session 5) made
     "the map looks sparse" attributable to the wrong component. Fixing visible bug A
     is often what makes underlying bug B measurable.

### 4.7 Decision: one goroutine per feed, coordinated by a WaitGroup

- **Context.** Session 3 added the alerts feed. Options: interleave both fetches in one
  loop; two separate processes; or two goroutines in one process.
- **Decision.** `go pollVehicles(...)` + `go pollAlerts(...)` + `wg.Wait()`. Each poller
  owns its own ticker, its own writer, its own state (the dedup map), and shares only
  the read-only HTTP client. Different cadences (15s/30s) stay trivially independent — a
  slow alerts fetch can never delay a vehicles poll.
- **Why not fancier.** No channels-based pipeline, no worker pools, no context
  cancellation tree — the problem is two independent periodic tasks, and the simplest
  concurrent structure that expresses that is two goroutines that never communicate.
  The WaitGroup exists only to park `main` forever.
- **Lesson.** *Concurrency structure should mirror problem structure.* Two independent
  cadences → two independent loops. The absence of shared mutable state is what makes
  the "no mutex anywhere" property safe, and that property came from design, not luck.

### 4.8 Minor but real: the Windows dev-loop fights

Not code bugs, but they shaped how this service is run: the machine's
`NoDefaultCurrentDirectoryInExePath=1` plus `make` executing recipes via `cmd.exe` broke
every variant of "build then run the binary" (`KAFKA_BROKER=... ./bin` is POSIX-only
syntax; `./ingestion-service.exe` mis-tokenizes in cmd; bare `ingestion-service.exe`
is blocked by that env var). The fix was to make the Makefile target just `go run .` —
only PATH-resolved commands, no path-prefixed executables. Lesson: *dev tooling is part
of the system; "works on my machine" cuts both ways and the Makefile is code too.*

---

## 5. Concepts glossary (as used in this service)

**Kafka producer.** A client that appends messages to Kafka topics. Here it's
`kafka.Writer`: it takes (key, value) byte pairs, groups them into batches, and sends
each batch to the broker leading the target partition, blocking until acknowledged. The
producer never knows who consumes — that decoupling is why five other services can be
added or restarted without this one caring.

**Kafka topic / partition / key.** A topic is a named, append-only log;
`vehicle-positions` has 3 partitions, meaning the log is split three ways for
parallelism (downstream, up to 3 consumer instances in a group can share the work). A
message's key (here `vehicle_id`) is hashed to pick its partition, which guarantees all
events for one vehicle stay in order relative to each other — but *not* ordered across
different vehicles. Choosing the key is choosing your ordering and scaling unit.

**Compacted topic.** Normal topics delete data by age; a compacted topic
(`service-alerts`) keeps *the latest record per key* indefinitely, deleting only
superseded versions. It turns a topic into a durable last-known-state table — a new
consumer replaying it gets every alert's current version. Compaction is lazy/eventual,
which is why the ripple detector consumes this topic as a KTable (upsert semantics by
key) instead of assuming duplicates are already gone.

**Schema Registry / subject / schema id.** A small HTTP service that stores Avro schemas
and assigns each unique schema an integer id. Producers register schemas under a
*subject* (`vehicle-positions-value`) and the registry enforces compatibility rules on
changes — it would have rejected our v2 evolution if the new fields hadn't been nullable
with defaults. Consumers use the id embedded in each message to fetch exactly the right
schema version. It's the contract-enforcement point between the Go producer and the Java
consumers.

**Avro / Confluent wire format.** Avro is a compact binary serialization format where
the schema lives *outside* the data — messages carry no field names, so they're small,
but you must know the writer's schema to decode. The Confluent wire format is the
5-byte prefix (magic `0x00` + big-endian schema id) that tells consumers *which* schema
that is. This service builds those 5 bytes by hand in `encodeConfluentAvro`.

**Protobuf (Protocol Buffers).** Google's binary serialization format, schema-first like
Avro but with field tags embedded in the bytes. It's the *input* format here — GTFS-RT
is published as protobuf — and Avro is the *output* format. So this service is, at its
core, a protobuf-to-Avro transcoder with a scheduler attached. Generated Go bindings
give typed accessors (`GetPosition()`, `GetTrip()`) with pointer-based optionality.

**GTFS and GTFS-Realtime.** GTFS ("static") is the standard zip-of-CSVs describing a
transit network's routes, stops, and schedules — loaded by `gtfs-loader` into
TimescaleDB. GTFS-Realtime is its live companion spec: protobuf feeds for vehicle
positions, trip updates, and service alerts. Knowing that `direction_id` is optional in
GTFS-RT — and that the TTC doesn't send it — drove a real design workaround downstream.

**CGO.** Go's mechanism for calling C code. It makes builds require a C toolchain,
complicates cross-compilation, and breaks the "one static binary" story
(`CGO_ENABLED=0` in the Dockerfile forces it off). It's why `confluent-kafka-go`
(which wraps the C library `librdkafka`) was swapped for pure-Go `segmentio/kafka-go`
in this project's first real roadblock.

**Goroutine.** A function running concurrently, managed by the Go runtime rather than
the OS — starts with the `go` keyword, costs kilobytes, and is multiplexed onto OS
threads. This service runs three: `main` (parked on `wg.Wait()`), the vehicles poller,
and the alerts poller. Java analog: virtual threads, but syntactically first-class.

**Channel.** Go's built-in typed queue for goroutine communication; reading blocks until
a value arrives. The only channel used here is `time.Ticker.C` — the ticker goroutine
sends a timestamp every interval and `for range ticker.C` receives them, which is how
"do this every 15 seconds" is expressed without a scheduler framework.

**`sync.WaitGroup`.** A counter you `Add` to, `Done` (decrement) from goroutines, and
`Wait` on until it hits zero — Java's `CountDownLatch`. Used here for exactly one thing:
keeping `main` from returning (which would kill the whole process, goroutines included).

**`defer`.** Schedules a call to run when the enclosing function returns, in LIFO order —
`finally` declared at acquisition site. The idiomatic pairing is resource-acquire on one
line, `defer release()` on the next (`resp.Body.Close()`, `ticker.Stop()`), so cleanup
can't be forgotten in any return path.

**Errors as values.** Go functions return `error` as an ordinary value instead of
throwing; call sites decide fatal-vs-recoverable explicitly. In this service the policy
is visible in the code: startup errors are fatal (`log.Fatalf` — no point running
without schemas), per-message errors skip the message, per-poll errors skip the poll and
rely on the next tick as the retry.

**Multi-stage Docker build.** Building in a heavyweight image (Go toolchain, ~800MB) and
copying only the artifact into a minimal runtime image (Alpine, ~7MB + binary). Also the
home of the layer-caching pattern: copy dependency manifests and download deps *before*
copying source, so code edits don't re-download the world.

**Advertised listeners (Kafka).** The broker addresses Kafka *tells clients to use*
after bootstrap — distinct from the address the client first dialed. The reason
containers must use `kafka:29092` while the host uses `localhost:9092`, and the answer
to the interview classic "my containerized client connects but gets no data."

**At-least-once delivery (as this service practices it).** Nothing here guarantees
exactly-once: a crash between a successful batch write and the next poll can republish
data, and alert dedup state resets on restart. Instead of fighting that, the system
makes duplicates harmless — vehicle updates are idempotent snapshots keyed by id, alerts
are upserts on a compacted topic. Making consumers idempotent is usually cheaper than
making producers perfect, and this repo is a working example.
