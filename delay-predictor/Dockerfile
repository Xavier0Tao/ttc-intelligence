# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /app

COPY pom.xml .
RUN mvn -q -B dependency:go-offline

COPY src ./src
RUN mvn -q -B package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre-alpine

# Kafka Streams state stores use RocksDB via JNI, which needs libstdc++
RUN apk add --no-cache libstdc++

WORKDIR /app

COPY --from=build /app/target/delay-predictor-1.0.0-SNAPSHOT-jar-with-dependencies.jar app.jar

CMD ["java", "-jar", "app.jar"]
