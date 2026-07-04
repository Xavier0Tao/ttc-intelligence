# Build context is the monorepo root so the shared schemas/ directory is available
# to avro-maven-plugin (which reads ../schemas relative to the pom).
# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build/crowding-estimator

COPY crowding-estimator/pom.xml .
RUN mvn -q -B dependency:go-offline

COPY schemas /build/schemas
COPY crowding-estimator/src ./src
RUN mvn -q -B package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre-alpine

# Kafka Streams state stores use RocksDB via JNI, which needs libstdc++
RUN apk add --no-cache libstdc++

WORKDIR /app

COPY --from=build /build/crowding-estimator/target/crowding-estimator-1.0.0-SNAPSHOT-jar-with-dependencies.jar app.jar

CMD ["java", "-jar", "app.jar"]
