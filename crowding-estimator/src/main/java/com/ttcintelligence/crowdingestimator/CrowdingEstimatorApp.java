package com.ttcintelligence.crowdingestimator;

import ca.ttc.intelligence.VehiclePosition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
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
 * Kafka Streams application that estimates per-vehicle crowding from headway
 * gaps. Vehicles on the same route and direction are windowed together; a
 * vehicle with a large gap to the vehicle ahead of it has had more time for
 * passengers to accumulate at its upcoming stops, so it is likely crowded.
 */
public class CrowdingEstimatorApp {

    private static final Logger log = LoggerFactory.getLogger(CrowdingEstimatorApp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static final Duration WINDOW_GRACE = Duration.ofSeconds(30);
    private static final ZoneId TORONTO = ZoneId.of("America/Toronto");

    private static final double CROWDED_RATIO = 1.5;
    private static final double EMPTY_RATIO = 0.5;

    public static void main(String[] args) {
        Properties config = AppConfig.load();
        String bootstrapServers = config.getProperty("kafka.bootstrap.servers");
        String schemaRegistryUrl = config.getProperty("kafka.schema.registry.url");
        String applicationId = config.getProperty("kafka.application.id");
        String inputTopic = config.getProperty("kafka.input.topic");
        String outputTopic = config.getProperty("kafka.output.topic");

        TimescaleDBClient db = new TimescaleDBClient();

        // The TTC GTFS-RT feed does not populate trip.direction_id, so we
        // resolve direction from the static GTFS trips table instead.
        Map<String, Integer> tripDirections = db.loadTripDirections();
        if (tripDirections.isEmpty()) {
            log.warn("trips table is empty — direction can only come from the feed, "
                    + "which the TTC does not populate; most records will be dropped. "
                    + "Run `make load-gtfs` to populate it.");
        } else {
            log.info("loaded {} trip->direction mappings from TimescaleDB", tripDirections.size());
        }

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // The topic may contain non-Avro records (e.g. from earlier JSON
        // producers); skip them instead of crashing the application.
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        KafkaStreams streams = new KafkaStreams(
                buildTopology(inputTopic, outputTopic, schemaRegistryUrl, db, tripDirections), streamsProps);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            db.close();
            latch.countDown();
        }));

        log.info("starting crowding-estimator: {} -> {} (bootstrap={}, schema-registry={})",
                inputTopic, outputTopic, bootstrapServers, schemaRegistryUrl);
        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static Topology buildTopology(String inputTopic, String outputTopic, String schemaRegistryUrl,
                                  TimescaleDBClient db, Map<String, Integer> tripDirections) {

        Serde<VehiclePosition> vehicleSerde = vehiclePositionSerde(schemaRegistryUrl);
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.String(), vehicleSerde))
                .filter((key, vp) -> vp != null
                        && vp.getRouteId() != null && !vp.getRouteId().isEmpty()
                        && vp.getVehicleId() != null && !vp.getVehicleId().isEmpty()
                        && vp.getCurrentStopSequence() != null
                        && resolveDirection(vp, tripDirections) != null)
                .groupBy((key, vp) -> vp.getRouteId() + ":" + resolveDirection(vp, tripDirections),
                        Grouped.with(Serdes.String(), vehicleSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE))
                .aggregate(
                        HashMap::new,
                        (routeDirection, vp, vehicles) -> {
                            VehicleSnapshot previous = vehicles.get(vp.getVehicleId());
                            if (previous == null || vp.getTimestamp() >= previous.timestamp) {
                                vehicles.put(vp.getVehicleId(),
                                        new VehicleSnapshot(vp.getCurrentStopSequence(), vp.getTimestamp()));
                            }
                            return vehicles;
                        },
                        Materialized.with(Serdes.String(), new VehicleMapSerde()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .flatMap((windowedKey, vehicles) -> buildEstimates(windowedKey, vehicles, db))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    /**
     * Resolves the direction of travel: from the event when present, otherwise
     * from the static-GTFS trip lookup. Null when neither source knows.
     */
    private static Integer resolveDirection(VehiclePosition vp, Map<String, Integer> tripDirections) {
        if (vp.getDirectionId() != null) {
            return vp.getDirectionId();
        }
        if (vp.getTripId() != null) {
            return tripDirections.get(vp.getTripId());
        }
        return null;
    }

    /**
     * Emits one crowding estimate per vehicle for a closed window of one
     * route+direction.
     */
    private static List<KeyValue<String, String>> buildEstimates(
            Windowed<String> windowedKey, Map<String, VehicleSnapshot> vehicles, TimescaleDBClient db) {

        List<KeyValue<String, String>> out = new ArrayList<>();

        String composite = windowedKey.key();
        int split = composite.lastIndexOf(':');
        String routeId = composite.substring(0, split);
        int directionId = Integer.parseInt(composite.substring(split + 1));
        long windowEnd = windowedKey.window().end();

        int hourOfDay = ZonedDateTime.ofInstant(Instant.ofEpochMilli(windowEnd), TORONTO).getHour();
        OptionalDouble scheduled = db.getScheduledHeadway(routeId, hourOfDay);
        if (scheduled.isEmpty()) {
            log.warn("no scheduled headway for route={} hour={}; skipping {} vehicle estimates",
                    routeId, hourOfDay, vehicles.size());
            return out;
        }
        double scheduledHeadway = scheduled.getAsDouble();

        // Sort vehicles by position along the route (ascending stop sequence).
        List<Map.Entry<String, VehicleSnapshot>> sorted = new ArrayList<>(vehicles.entrySet());
        sorted.sort((a, b) -> Integer.compare(a.getValue().stopSequence, b.getValue().stopSequence));

        // Rough time-per-stop estimate: assume the fleet, spread over
        // (maxSeq - minSeq) stops, covers that span in one window length.
        // minutes_per_stop = window_minutes / span. This ignores actual travel
        // speed and stop spacing — it is only meant to turn "stops of gap"
        // into an order-of-magnitude "minutes of gap".
        int span = sorted.get(sorted.size() - 1).getValue().stopSequence
                - sorted.get(0).getValue().stopSequence;
        Double minutesPerStop = span > 0 ? (double) WINDOW_SIZE.toMinutes() / span : null;

        long computedAt = System.currentTimeMillis();
        for (int i = 0; i < sorted.size(); i++) {
            String vehicleId = sorted.get(i).getKey();

            // The vehicle "ahead" is the next one along the route (higher stop
            // sequence). The leading vehicle has nothing ahead -> null gap.
            Integer gapStops = null;
            if (i < sorted.size() - 1) {
                gapStops = sorted.get(i + 1).getValue().stopSequence
                        - sorted.get(i).getValue().stopSequence;
            }

            Double gapMinutes = (gapStops != null && minutesPerStop != null)
                    ? gapStops * minutesPerStop : null;
            Double ratio = gapMinutes != null ? gapMinutes / scheduledHeadway : null;

            ObjectNode json = MAPPER.createObjectNode();
            json.put("vehicle_id", vehicleId);
            json.put("route_id", routeId);
            json.put("direction_id", directionId);
            if (gapMinutes != null) {
                json.put("gap_ahead_minutes", round2(gapMinutes));
                json.put("crowding_ratio", round2(ratio));
            } else {
                json.putNull("gap_ahead_minutes");
                json.putNull("crowding_ratio");
            }
            json.put("scheduled_headway_minutes", round2(scheduledHeadway));
            json.put("crowding_level", classify(ratio));
            json.put("window_end", windowEnd);
            json.put("computed_at", computedAt);

            out.add(KeyValue.pair(vehicleId, json.toString()));
        }

        log.info("route={} direction={} window_end={} vehicles={} estimates published",
                routeId, directionId, windowEnd, out.size());
        return out;
    }

    private static String classify(Double ratio) {
        if (ratio == null) {
            return "UNKNOWN";
        }
        if (ratio >= CROWDED_RATIO) {
            return "LIKELY_CROWDED";
        }
        if (ratio <= EMPTY_RATIO) {
            return "LIKELY_EMPTY";
        }
        return "NORMAL";
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
