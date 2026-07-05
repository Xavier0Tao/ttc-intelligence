# TTC Intelligence

A real-time transit intelligence platform for the Toronto Transit Commission (TTC). It ingests live vehicle position data from TTC's public GTFS-RT feed, streams it through Kafka, processes it with a set of Kafka Streams microservices, and serves the results to a live map dashboard over WebSocket and REST.

## Architecture

```
TTC GTFS-RT feed (protobuf, polled every 15s)
        │
        ▼
 ingestion-service (Go)  ──publish──▶  Kafka topic: vehicle-positions
                                              │
                    ┌─────────────────────────┼─────────────────────────┐
                    ▼                         ▼                         │
          delay-predictor (Java)   crowding-estimator (Java)            │
          (Kafka Streams)          (Kafka Streams)                     │
                    │                         │                        │
                    ▼                         ▼                        │
       Kafka topic: delay-predictions   Kafka topic: crowding-estimates │
                                                                        │
 Kafka topic: service-alerts ──▶ ripple-detector (Java, Kafka Streams) │
                                        │                               │
                                        ▼                               │
                            Kafka topic: ripple-alerts                  │
                                                                        │
        ┌───────────────────────────────────────────────────────────────┘
        ▼
 api-gateway (Java + Spring Boot)
   - consumes all output topics
   - caches live vehicle state in Redis
   - persists historical delay data in TimescaleDB
   - serves REST + WebSocket to the dashboard
        │
        ▼
 dashboard (React) — live Toronto map: vehicle positions, delay heatmap, alert feed
```

### Components

| Component | Stack | Responsibility |
|---|---|---|
| `ingestion-service` | Go | Polls the TTC GTFS-RT feed every 15s, parses the protobuf payload, publishes each vehicle position as a JSON event to `vehicle-positions`. |
| `delay-predictor` | Java, Kafka Streams | Consumes `vehicle-positions`, compares actual headway against the static GTFS schedule, publishes delay scores to `delay-predictions`. |
| `ripple-detector` | Java, Kafka Streams | Consumes `service-alerts`, identifies downstream surface routes affected by a disruption, publishes cascade alerts to `ripple-alerts`. |
| `crowding-estimator` | Java, Kafka Streams | Consumes `vehicle-positions`, infers crowding from headway gaps between vehicles, publishes to `crowding-estimates`. |
| `api-gateway` | Java, Spring Boot | Consumes all output topics, serves WebSocket + REST endpoints to the dashboard. |
| `dashboard` | React | Live Toronto map showing vehicle positions, a delay heatmap, and an alert feed. |

## Tech stack rationale

- **Apache Kafka** — the backbone of the system. Decouples ingestion from processing so each microservice can consume, scale, and fail independently. Chosen specifically to demonstrate stream-processing fundamentals (topics, partitions, consumer groups) relevant to companies like Confluent.
- **Go for ingestion** — a single lightweight binary that polls an external HTTP endpoint and produces to Kafka is a natural fit for Go's concurrency model and small container footprint (two-stage Docker build keeps the runtime image minimal).
- **Java + Kafka Streams for processing** — Kafka Streams gives each processing service a stateful, exactly-once (per-topic) stream-processing model without needing an external processing cluster (e.g. Spark/Flink), keeping the system self-contained while still showcasing real stream-processing patterns (windowing, joins, stateful aggregation).
- **Plain Maven, no Spring Boot, for the stream processors** — these services do one narrow job (consume, transform, produce) and don't need a DI container or web server; keeping them as plain JVM processes keeps startup fast and the dependency graph small.
- **Spring Boot for the API gateway** — the gateway is the one component that needs a web/WebSocket server, REST routing, and integration glue (`spring-kafka`), which is exactly what Spring Boot is built for.
- **Redis** — low-latency cache for the current position of every vehicle, so the dashboard/WebSocket layer can serve "live state" reads without hitting Kafka or the database.
- **TimescaleDB** — a Postgres extension for time-series data, used to persist historical delay/crowding data for trend queries, while still giving full SQL/Postgres compatibility.
- **React** — component-driven UI well suited to a live map with frequently updating markers and overlays.
- **Kubernetes (Minikube)** — not needed yet; reserved for a later phase once the services run reliably via Docker Compose.

## Repository layout

```
ttc-intelligence/
├── schemas/               Shared Avro schemas (single source of truth)
├── ingestion-service/     Go — GTFS-RT vehicles + alerts pollers / Avro Kafka producer
├── gtfs-loader/           Python — one-shot static GTFS → TimescaleDB loader
├── delay-predictor/       Java + Kafka Streams — headway/delay scoring
├── crowding-estimator/    Java + Kafka Streams — per-vehicle crowding levels
├── ripple-detector/       Java + Kafka Streams — cascades subway alerts to feeder routes
├── api-gateway/           Java + Spring Boot — Kafka→Redis sync, WebSocket firehose, REST
├── scripts/               Demo tooling (service-alert injector)
├── dashboard/             React + Vite + Leaflet — live map, delay panel, alert feed
├── k8s/                   Kubernetes manifests (not yet written)
├── docker-compose.yml     Kafka, Zookeeper, Schema Registry, Redis, TimescaleDB + app services
├── Makefile
└── README.md
```

## Event serialization

Events are serialized as **Avro** using the Confluent wire format (magic byte + 4-byte schema id + Avro binary). All schemas live in the shared `schemas/` directory — `vehicle_position.avsc` (subject `vehicle-positions-value`, currently at version 2 after a backward-compatible evolution adding `trip_id`/`direction_id`/`current_stop_sequence`) and `service_alert.avsc` (subject `service-alerts-value`). The ingestion service registers both at startup; the Java services generate their record classes from the same files at build time via `avro-maven-plugin` (`../schemas` relative to each pom).

One TTC-specific wrinkle: the live feed populates `trip_id` and `current_stop_sequence` but **not** `direction_id`. The GTFS loader therefore also loads a `trips` (trip_id → direction_id) lookup table, which the crowding estimator holds in memory to resolve direction per vehicle.

`service-alerts` and `ripple-alerts` are **compacted topics** keyed by `alert_id` / `ripple_id`: the latest state per alert is retained indefinitely while superseded versions are eventually cleaned. The ripple detector consumes `service-alerts` as a KTable, giving upsert-by-key semantics regardless of compaction timing.

## Static data (TimescaleDB)

The `gtfs-loader` downloads the City of Toronto's **merged GTFS** feed (all modes — subway, streetcar, bus) and full-refreshes four tables: `routes`, `trips` (direction lookup), `scheduled_headways` (per route number per hour, weekday service, busier direction), and `station_feeder_routes` — a geospatial mapping of which surface routes stop within 300 m (haversine) of each of the 70 subway stations. Subway stations are identified as parent stations of `route_type=1`-served platforms, because `location_type=1` alone also marks bus terminal loops in the TTC feed.

## Local setup

### Prerequisites

- Docker + Docker Compose
- Go 1.22+ (only for running the ingestion service outside Docker)
- Node.js 20+ (once the dashboard is scaffolded)

### 1. Start the stack

```bash
make up
```

This starts Zookeeper, Kafka (broker reachable at `localhost:9092`), Schema Registry (`localhost:8081`), Redis, TimescaleDB, the containerized ingestion service (vehicles + alerts pollers), the delay predictor, and the crowding estimator. Kafka topics (`vehicle-positions`, `service-alerts`, `delay-predictions`, `ripple-alerts`, `crowding-estimates`) are created by a one-shot `kafka-init` container, and the static GTFS schedule is loaded by a one-shot `gtfs-loader` container.

Useful targets:

```bash
make status          # git status + containers + Kafka topics
make logs            # all logs
make logs-ingest     # ingestion service only
make logs-delay      # delay predictor only
make logs-crowding   # crowding estimator only
make logs-ripple     # ripple detector only
make schema-list     # registered Schema Registry subjects
make down            # stop everything
```

### 2. Load / refresh the static GTFS schedule

```bash
make load-gtfs
```

Downloads the TTC static GTFS feed, computes scheduled headways per route per hour of day (weekday service, busier direction), and full-refreshes the `scheduled_headways` and `routes` tables in TimescaleDB. Re-run whenever TTC publishes a new schedule.

### 3. Read the output topics

```bash
make kafka-read           # live vehicle-position events (Avro → JSON)
make kafka-read-delays    # per-route delay predictions
make kafka-read-crowding  # per-vehicle crowding estimates
make kafka-read-alerts    # service alerts (real + injected)
make kafka-read-ripple    # cascade alerts for feeder routes
```

Ripple alerts — one message per feeder route affected by a subway alert:

```json
{"ripple_id":"DEMO-1783225103-97","source_alert_id":"DEMO-1783225103","source_effect":"NO_SERVICE","affected_station":"Bloor-Yonge","feeder_route_id":"97","predicted_impact":"LIKELY_CROWDING_INCREASE","header_text":"Feeder route 97 near Bloor-Yonge station: Line 1: No service between Bloor-Yonge and St George","detected_at":1783225104572}
```

Delay predictions — one message per active route per 5-minute window:

```json
{"route_id":"504","window_start":1719530400000,"window_end":1719530700000,"actual_headway_minutes":8.5,"scheduled_headway_minutes":5.0,"delay_score":3.5,"computed_at":1719530712000}
```

Crowding estimates — one message per vehicle per window, classified from the headway gap to the vehicle ahead:

```json
{"vehicle_id":"4218","route_id":"504","direction_id":0,"gap_ahead_minutes":9.2,"crowding_ratio":1.84,"scheduled_headway_minutes":5.0,"crowding_level":"LIKELY_CROWDED","window_end":1719530700000,"computed_at":1719530712000}
```

### 4. Inject a demo service alert

```bash
make inject-alert
```

Publishes a fake `NO_SERVICE` alert for Line 1 through the same Avro schema / Schema Registry path as real alerts — useful when the live TTC alerts feed is quiet. Override any field via env vars (`ALERT_HEADER`, `ALERT_EFFECT`, `ALERT_ROUTES`, `ALERT_STOPS`, `ALERT_DESCRIPTION`, ...), e.g.:

```bash
docker-compose run --rm -e ALERT_HEADER="Line 2: Delays at Kennedy" -e ALERT_ROUTES=2 alert-injector
```

### 5. Live dashboard

`make up` includes the API gateway (`localhost:8080`) and the dashboard (`http://localhost:3000`):

- **Gateway** consumes all five topics into Redis (`vehicle:*` TTL 120s, `crowding:*` TTL 10m, `delay:*`, `alert:*`, `ripple:*` TTL 30m) and serves a WebSocket firehose at `/ws/live` (snapshot on connect, then tagged updates) plus REST: `/api/routes`, `/api/routes/{route}/stops?direction=0`, `/api/snapshot`, `/api/alerts/active`. Smoke-test with `make api-test`.
- **Dashboard** is a Vite/React/Leaflet app served by nginx, which also proxies `/api` and `/ws` to the gateway (single origin). Vehicles render as type-specific silhouettes (bus/streetcar/subway, inferred from `route_type`) in TTC red with a crowding-colored ring (green NORMAL / red LIKELY_CROWDED / blue LIKELY_EMPTY / gray UNKNOWN). Hovering a route in the delay panel overlays that route's stops. The alert feed distinguishes cascaded ripple alerts, and the connection indicator reconnects with exponential backoff (1s→30s), resyncing via `/api/snapshot`.

### Running the ingestion service outside Docker (dev loop)

```bash
make ingest
```

Runs the Go service locally against `localhost:9092` / `localhost:8081`. The same events flow into the same topic, so the rest of the pipeline behaves identically.

### 3. Processing services and API gateway

Each Java service is a standalone Maven project. Build and run any of them with:

```bash
cd delay-predictor && mvn clean package && java -jar target/delay-predictor-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

The same pattern applies to `ripple-detector` and `crowding-estimator`. These are currently stubs (empty `main()`), pending stream-topology implementation.

The API gateway is a Spring Boot app:

```bash
cd api-gateway && mvn spring-boot:run
```

### 4. Dashboard

Not yet scaffolded.
