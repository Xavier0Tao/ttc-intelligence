# Build stage
FROM golang:1.22-alpine AS build

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o ingestion-service .

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=build /app/ingestion-service .

ENV KAFKA_BROKER=kafka:29092

CMD ["./ingestion-service"]
