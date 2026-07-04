# Build context is the monorepo root so the shared schemas/ directory is available.
# Build stage
FROM golang:1.22-alpine AS build

WORKDIR /app

COPY ingestion-service/go.mod ingestion-service/go.sum* ./
RUN go mod download

COPY ingestion-service/ .

RUN CGO_ENABLED=0 GOOS=linux go build -o ingestion-service .

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=build /app/ingestion-service .
COPY schemas /app/schemas

ENV KAFKA_BROKER=kafka:29092
ENV SCHEMAS_DIR=/app/schemas

CMD ["./ingestion-service"]
