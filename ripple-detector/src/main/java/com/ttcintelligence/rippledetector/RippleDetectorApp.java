package com.ttcintelligence.rippledetector;

import ca.ttc.intelligence.ServiceAlert;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams application that cascades subway service alerts onto the
 * surface routes that feed the affected stations. When a station loses subway
 * service, its feeder bus/streetcar routes absorb the displaced riders — this
 * service makes that ripple effect visible as first-class events.
 *
 * service-alerts is consumed as a KTable keyed by alert_id: the topic is
 * compacted, and table semantics give upsert-by-key regardless of compaction
 * timing, so reprocessing the same alert never duplicates downstream ripples
 * (output is additionally keyed by deterministic ripple_id on a compacted
 * topic).
 */
public class RippleDetectorApp {

    private static final Logger log = LoggerFactory.getLogger(RippleDetectorApp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

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

        log.info("starting ripple-detector: {} -> {} (bootstrap={}, schema-registry={})",
                inputTopic, outputTopic, bootstrapServers, schemaRegistryUrl);
        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static Topology buildTopology(String inputTopic, String outputTopic,
                                  String schemaRegistryUrl, TimescaleDBClient db) {

        Serde<ServiceAlert> alertSerde = serviceAlertSerde(schemaRegistryUrl);
        StreamsBuilder builder = new StreamsBuilder();

        builder.table(inputTopic, Consumed.with(Serdes.String(), alertSerde))
                .toStream()
                .flatMap((alertId, alert) -> buildRipples(alertId, alert, db))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    /**
     * Expands one alert (table upsert) into cascade alerts, one per feeder
     * route of the affected stations. Returns an empty list for tombstones
     * and for alerts that touch no known subway station.
     */
    private static List<KeyValue<String, String>> buildRipples(
            String alertId, ServiceAlert alert, TimescaleDBClient db) {

        List<KeyValue<String, String>> ripples = new ArrayList<>();
        if (alert == null) {
            // Compacted-topic tombstone: the alert was deleted upstream.
            return ripples;
        }

        List<FeederRoute> feeders = db.getFeedersForStations(alert.getAffectedStopIds());
        if (feeders.isEmpty()) {
            // Expected for alerts that aren't about subway stations (surface
            // detours, elevator outages at unlisted stops, weather, ...).
            log.warn("alert {} matched no stations in station_feeder_routes "
                            + "(affected_stop_ids={}); no ripples emitted",
                    alertId, alert.getAffectedStopIds());
            return ripples;
        }

        // The subway route the alert is about must not appear as its own feeder.
        Set<String> alertRoutes = new HashSet<>(alert.getAffectedRouteIds());

        // One ripple per feeder route; when a route feeds several affected
        // stations, keep the station where it comes closest.
        Map<String, FeederRoute> byRoute = new HashMap<>();
        for (FeederRoute feeder : feeders) {
            if (alertRoutes.contains(feeder.routeId())) {
                continue;
            }
            FeederRoute existing = byRoute.get(feeder.routeId());
            if (existing == null || feeder.distanceMeters() < existing.distanceMeters()) {
                byRoute.put(feeder.routeId(), feeder);
            }
        }

        long detectedAt = System.currentTimeMillis();
        for (FeederRoute feeder : byRoute.values()) {
            String rippleId = alertId + "-" + feeder.routeId();

            ObjectNode json = MAPPER.createObjectNode();
            json.put("ripple_id", rippleId);
            json.put("source_alert_id", alertId);
            json.put("source_effect", alert.getEffect());
            json.put("affected_station", feeder.stationName());
            json.put("feeder_route_id", feeder.routeId());
            json.put("predicted_impact", "LIKELY_CROWDING_INCREASE");
            json.put("header_text", "Feeder route " + feeder.routeId() + " near "
                    + feeder.stationName() + " station: " + alert.getHeaderText());
            json.put("detected_at", detectedAt);

            ripples.add(KeyValue.pair(rippleId, json.toString()));
        }

        log.info("alert {} ({}) cascaded to {} feeder route(s) across {} matched station row(s)",
                alertId, alert.getEffect(), ripples.size(), feeders.size());
        return ripples;
    }

    @SuppressWarnings("unchecked")
    private static Serde<ServiceAlert> serviceAlertSerde(String schemaRegistryUrl) {
        Map<String, Object> serdeConfig = Map.of(
                "schema.registry.url", schemaRegistryUrl,
                "specific.avro.reader", true);

        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(serdeConfig, false);
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(serdeConfig, false);

        return (Serde<ServiceAlert>) (Serde<?>) Serdes.serdeFrom(serializer, deserializer);
    }
}
