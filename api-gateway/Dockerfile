# Build context is the monorepo root so the shared schemas/ directory is available
# to avro-maven-plugin (which reads ../schemas relative to the pom).
# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build/api-gateway

COPY api-gateway/pom.xml .
RUN mvn -q -B dependency:go-offline

COPY schemas /build/schemas
COPY api-gateway/src ./src
RUN mvn -q -B package -DskipTests

# Runtime stage. Plain Kafka consumers, no Kafka Streams state stores, so no
# RocksDB and no libstdc++ needed here.
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY --from=build /build/api-gateway/target/api-gateway-1.0.0-SNAPSHOT.jar app.jar

EXPOSE 8080

CMD ["java", "-jar", "app.jar"]
