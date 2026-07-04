.PHONY: up down logs logs-ingest logs-delay ingest schema-list load-gtfs kafka-read-delays status

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

kafka-read-delays:
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic delay-predictions --from-beginning --max-messages 10 --timeout-ms 60000

status:
	@echo === Git status ===
	@git status
	@echo === Docker containers ===
	@docker ps --format "table {{.Names}}\t{{.Status}}"
	@echo === Kafka topics ===
	@docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list
