package com.ttcintelligence.delaypredictor;

import ca.ttc.intelligence.VehiclePosition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams application that consumes Avro vehicle positions, estimates
 * the actual headway per route over 5-minute tumbling windows, compares it
 * against the scheduled headway stored in TimescaleDB, and publishes a JSON
 * delay score per route per window to the delay-predictions topic.
 */
public class DelayPredictorApp {

    private static final Logger log = LoggerFactory.getLogger(DelayPredictorApp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static final Duration WINDOW_GRACE = Duration.ofSeconds(30);
    private static final ZoneId TORONTO = ZoneId.of("America/Toronto");

    public static void main(String[] args) {
        Properties config = AppConfig.load();
        String bootstrapServers = config.getProperty("kafka.bootstrap.servers");
        String schemaRegistryUrl = config.getProperty("kafka.schema.registry.url");
        String applicationId = config.getProperty("kafka.application.id");
        String inputTopic = config.getProperty("kafka.input.topic");
        String outputTopic = config.getProperty("kafka.output.topic");

        TimescaleDBClient db = new TimescaleDBClient();

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // The topic may contain non-Avro records (e.g. from earlier JSON
        // producers); skip them instead of crashing the application.
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        KafkaStreams streams = new KafkaStreams(
                buildTopology(inputTopic, outputTopic, schemaRegistryUrl, db), streamsProps);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            db.close();
            latch.countDown();
        }));

        log.info("starting delay-predictor: {} -> {} (bootstrap={}, schema-registry={})",
                inputTopic, outputTopic, bootstrapServers, schemaRegistryUrl);
        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static org.apache.kafka.streams.Topology buildTopology(
            String inputTopic, String outputTopic, String schemaRegistryUrl, TimescaleDBClient db) {

        Serde<VehiclePosition> vehicleSerde = vehiclePositionSerde(schemaRegistryUrl);
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.String(), vehicleSerde))
                .filter((key, vp) -> vp != null && vp.getRouteId() != null && !vp.getRouteId().isEmpty())
                .groupBy((key, vp) -> vp.getRouteId(), Grouped.with(Serdes.String(), vehicleSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE))
                .aggregate(
                        HashSet::new,
                        (route, vp, vehicles) -> {
                            vehicles.add(vp.getVehicleId());
                            return vehicles;
                        },
                        Materialized.with(Serdes.String(), new StringSetSerde()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedRoute, vehicles) -> KeyValue.pair(
                        windowedRoute.key(), toPrediction(windowedRoute, vehicles, db)))
                .filter((route, prediction) -> prediction != null)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    /**
     * Builds the delay prediction JSON for one closed window, or null when the
     * route has no schedule entry for that hour (nothing to compare against).
     */
    private static String toPrediction(Windowed<String> windowedRoute, Set<String> vehicles, TimescaleDBClient db) {
        String routeId = windowedRoute.key();
        long windowStart = windowedRoute.window().start();
        long windowEnd = windowedRoute.window().end();

        int vehicleCount = vehicles.size();
        double actualHeadway = WINDOW_SIZE.toMinutes() / (double) vehicleCount;

        int hourOfDay = ZonedDateTime.ofInstant(Instant.ofEpochMilli(windowStart), TORONTO).getHour();
        OptionalDouble scheduled = db.getScheduledHeadway(routeId, hourOfDay);
        if (scheduled.isEmpty()) {
            log.debug("no scheduled headway for route={} hour={}; skipping window", routeId, hourOfDay);
            return null;
        }

        double delayScore = actualHeadway - scheduled.getAsDouble();

        ObjectNode json = MAPPER.createObjectNode();
        json.put("route_id", routeId);
        json.put("window_start", windowStart);
        json.put("window_end", windowEnd);
        json.put("actual_headway_minutes", round2(actualHeadway));
        json.put("scheduled_headway_minutes", round2(scheduled.getAsDouble()));
        json.put("delay_score", round2(delayScore));
        json.put("computed_at", System.currentTimeMillis());

        log.info("route={} window=[{}..{}] vehicles={} actual={}m scheduled={}m delay_score={}",
                routeId, windowStart, windowEnd, vehicleCount,
                round2(actualHeadway), round2(scheduled.getAsDouble()), round2(delayScore));
        return json.toString();
    }

    @SuppressWarnings("unchecked")
    private static Serde<VehiclePosition> vehiclePositionSerde(String schemaRegistryUrl) {
        Map<String, Object> serdeConfig = Map.of(
                "schema.registry.url", schemaRegistryUrl,
                "specific.avro.reader", true);

        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(serdeConfig, false);
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(serdeConfig, false);

        return (Serde<VehiclePosition>) (Serde<?>) Serdes.serdeFrom(serializer, deserializer);
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
