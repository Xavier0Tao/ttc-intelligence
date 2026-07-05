.PHONY: up down logs logs-ingest logs-delay logs-crowding logs-ripple ingest schema-list load-gtfs kafka-read kafka-read-delays kafka-read-alerts kafka-read-crowding kafka-read-ripple inject-alert status

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

logs-ingest:
	docker-compose logs -f ingestion-service

ingest:
	cd ingestion-service && go run .

schema-list:
	curl -s http://localhost:8081/subjects

load-gtfs:
	docker-compose build gtfs-loader && docker-compose run --rm gtfs-loader

logs-delay:
	docker-compose logs -f delay-predictor

kafka-read:
	docker-compose exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic vehicle-positions --max-messages 10 --timeout-ms 60000 --property schema.registry.url=http://localhost:8081

kafka-read-delays:
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic delay-predictions --from-beginning --max-messages 10 --timeout-ms 60000

logs-crowding:
	docker-compose logs -f crowding-estimator

kafka-read-crowding:
	docker-compose exec kafka bash -c "kafka-console-consumer --bootstrap-server localhost:9092 --topic crowding-estimates --from-beginning --timeout-ms 30000 2>/dev/null | tail -10"

logs-ripple:
	docker-compose logs -f ripple-detector

kafka-read-ripple:
	docker-compose exec kafka bash -c "kafka-console-consumer --bootstrap-server localhost:9092 --topic ripple-alerts --from-beginning --timeout-ms 30000 2>/dev/null | tail -10"

inject-alert:
	docker-compose build alert-injector && docker-compose run --rm alert-injector

kafka-read-alerts:
	docker-compose exec schema-registry bash -c "kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic service-alerts --from-beginning --timeout-ms 30000 --property schema.registry.url=http://localhost:8081 2>/dev/null | grep '^{' | tail -10"

status:
	@echo === Git status ===
	@git status
	@echo === Docker containers ===
	@docker ps --format "table {{.Names}}\t{{.Status}}"
	@echo === Kafka topics ===
	@docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list
