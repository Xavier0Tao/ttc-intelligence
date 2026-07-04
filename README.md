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
├── ingestion-service/     Go — GTFS-RT poller / Avro Kafka producer
├── gtfs-loader/           Python — one-shot static GTFS → TimescaleDB loader
├── delay-predictor/       Java + Kafka Streams — headway/delay scoring
├── ripple-detector/       Java + Kafka Streams (stub)
├── crowding-estimator/    Java + Kafka Streams (stub)
├── api-gateway/           Java + Spring Boot (stub)
├── dashboard/             React (not yet scaffolded)
├── k8s/                   Kubernetes manifests (not yet written)
├── docker-compose.yml     Kafka, Zookeeper, Schema Registry, Redis, TimescaleDB + app services
├── Makefile
└── README.md
```

## Event serialization

Vehicle position events are serialized as **Avro** using the Confluent wire format (magic byte + 4-byte schema id + Avro binary). The schema lives at `ingestion-service/avro/vehicle_position.avsc` (mirrored in `delay-predictor/src/main/avro/`) and is registered with **Schema Registry** under the subject `vehicle-positions-value` at ingestion startup. The delay predictor generates its `VehiclePosition` class from the same schema at build time via `avro-maven-plugin`.

## Local setup

### Prerequisites

- Docker + Docker Compose
- Go 1.22+ (only for running the ingestion service outside Docker)
- Node.js 20+ (once the dashboard is scaffolded)

### 1. Start the stack

```bash
make up
```

This starts Zookeeper, Kafka (broker reachable at `localhost:9092`), Schema Registry (`localhost:8081`), Redis, TimescaleDB, the containerized ingestion service, and the delay predictor. Kafka topics (`vehicle-positions`, `service-alerts`, `delay-predictions`, `ripple-alerts`, `crowding-estimates`) are created by a one-shot `kafka-init` container, and the static GTFS schedule is loaded by a one-shot `gtfs-loader` container.

Useful targets:

```bash
make logs          # all logs
make logs-ingest   # ingestion service only
make logs-delay    # delay predictor only
make schema-list   # registered Schema Registry subjects
make down          # stop everything
```

### 2. Load / refresh the static GTFS schedule

```bash
make load-gtfs
```

Downloads the TTC static GTFS feed, computes scheduled headways per route per hour of day (weekday service, busier direction), and full-refreshes the `scheduled_headways` and `routes` tables in TimescaleDB. Re-run whenever TTC publishes a new schedule.

### 3. Read delay predictions

```bash
make kafka-read-delays
```

Prints delay prediction JSON from the `delay-predictions` topic. One message per active route per 5-minute window:

```json
{"route_id":"504","window_start":1719530400000,"window_end":1719530700000,"actual_headway_minutes":8.5,"scheduled_headway_minutes":5.0,"delay_score":3.5,"computed_at":1719530712000}
```

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
