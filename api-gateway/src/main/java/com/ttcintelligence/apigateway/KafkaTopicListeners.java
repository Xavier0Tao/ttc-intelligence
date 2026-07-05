package com.ttcintelligence.apigateway;

import ca.ttc.intelligence.ServiceAlert;
import ca.ttc.intelligence.VehiclePosition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka -> Redis -> WebSocket relay, one listener per topic.
 *
 * Offset strategy: the high-volume stream topics (vehicle-positions,
 * delay-predictions, crowding-estimates) use a stable consumer group starting
 * at latest — their state refreshes within one poll/window anyway. The two
 * compacted alert topics use a fresh consumer group per boot starting at
 * earliest, so a gateway restart replays them and fully rebuilds the Redis
 * alert/ripple state (Redis is not persisted; the topics are small and
 * compacted). Replay also gives Redis upsert-by-key semantics equivalent to a
 * KTable without pulling Kafka Streams (and RocksDB state stores) into what
 * is otherwise a plain consumer application.
 */
@Component
public class KafkaTopicListeners {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicListeners.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RedisStateService redis;
    private final LiveWebSocketHandler ws;

    public KafkaTopicListeners(RedisStateService redis, LiveWebSocketHandler ws) {
        this.redis = redis;
        this.ws = ws;
    }

    @KafkaListener(topics = "vehicle-positions", groupId = "api-gateway",
            containerFactory = "avroFactory",
            properties = "auto.offset.reset=latest")
    public void onVehiclePosition(ConsumerRecord<String, Object> record) {
        Object value = record.value();
        if (!(value instanceof VehiclePosition vp)) {
            log.warn("vehicle-positions: unexpected value type {}",
                    value == null ? "null" : value.getClass().getName());
            return;
        }
        if (vp.getVehicleId() == null || vp.getVehicleId().isEmpty()) {
            return;
        }
        String json = vehicleToJson(vp).toString();
        redis.putVehicle(vp.getVehicleId(), json);
        ws.broadcast("vehicle_update", json);
    }

    @KafkaListener(topics = "delay-predictions", groupId = "api-gateway",
            containerFactory = "stringFactory",
            properties = "auto.offset.reset=latest")
    public void onDelayPrediction(String json) {
        String routeId = extractField(json, "route_id");
        if (routeId == null) {
            return;
        }
        redis.putDelay(routeId, json);
        ws.broadcast("delay_update", json);
    }

    @KafkaListener(topics = "crowding-estimates", groupId = "api-gateway",
            containerFactory = "stringFactory",
            properties = "auto.offset.reset=latest")
    public void onCrowdingEstimate(String json) {
        String vehicleId = extractField(json, "vehicle_id");
        if (vehicleId == null) {
            return;
        }
        redis.putCrowding(vehicleId, json);
        ws.broadcast("crowding_update", json);
    }

    @KafkaListener(topics = "service-alerts", groupId = "api-gateway-alerts-${random.uuid}",
            containerFactory = "avroFactory",
            properties = "auto.offset.reset=earliest")
    public void onServiceAlert(ConsumerRecord<String, Object> record) {
        Object value = record.value();
        if (!(value instanceof ServiceAlert alert)) {
            log.warn("service-alerts: unexpected value type {}",
                    value == null ? "null" : value.getClass().getName());
            return;
        }
        if (alert.getAlertId() == null) {
            return;
        }
        String json = alertToJson(alert).toString();
        redis.putAlert(alert.getAlertId(), json);
        ws.broadcast("alert_update", json);
    }

    @KafkaListener(topics = "ripple-alerts", groupId = "api-gateway-ripples-${random.uuid}",
            containerFactory = "stringFactory",
            properties = "auto.offset.reset=earliest")
    public void onRippleAlert(String json) {
        String rippleId = extractField(json, "ripple_id");
        if (rippleId == null) {
            return;
        }
        redis.putRipple(rippleId, json);
        ws.broadcast("ripple_update", json);
    }

    private String extractField(String json, String field) {
        try {
            String value = MAPPER.readTree(json).path(field).asText("");
            return value.isEmpty() ? null : value;
        } catch (Exception e) {
            log.warn("unparseable message on {} lookup: {}", field, e.getMessage());
            return null;
        }
    }

    private static ObjectNode vehicleToJson(VehiclePosition vp) {
        ObjectNode json = MAPPER.createObjectNode();
        json.put("vehicle_id", vp.getVehicleId());
        json.put("route_id", vp.getRouteId());
        json.put("latitude", vp.getLatitude());
        json.put("longitude", vp.getLongitude());
        if (vp.getBearing() != null) {
            json.put("bearing", vp.getBearing());
        }
        if (vp.getSpeed() != null) {
            json.put("speed", vp.getSpeed());
        }
        json.put("timestamp", vp.getTimestamp());
        if (vp.getOccupancy() != null) {
            json.put("occupancy", vp.getOccupancy());
        }
        if (vp.getTripId() != null) {
            json.put("trip_id", vp.getTripId());
        }
        if (vp.getDirectionId() != null) {
            json.put("direction_id", vp.getDirectionId());
        }
        if (vp.getCurrentStopSequence() != null) {
            json.put("current_stop_sequence", vp.getCurrentStopSequence());
        }
        return json;
    }

    private static ObjectNode alertToJson(ServiceAlert alert) {
        ObjectNode json = MAPPER.createObjectNode();
        json.put("alert_id", alert.getAlertId());
        json.put("effect", alert.getEffect());
        if (alert.getCause() != null) {
            json.put("cause", alert.getCause());
        }
        json.put("header_text", alert.getHeaderText());
        if (alert.getDescriptionText() != null) {
            json.put("description_text", alert.getDescriptionText());
        }
        ArrayNode routes = json.putArray("affected_route_ids");
        alert.getAffectedRouteIds().forEach(routes::add);
        ArrayNode stops = json.putArray("affected_stop_ids");
        alert.getAffectedStopIds().forEach(stops::add);
        json.put("active_since", alert.getActiveSince());
        json.put("timestamp", alert.getTimestamp());
        return json;
    }
}
