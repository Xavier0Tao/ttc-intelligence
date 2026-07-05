package com.ttcintelligence.apigateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Assembles the full current state from Redis into the snapshot payload sent
 * to WebSocket clients on connect and served at GET /api/snapshot. Crowding
 * levels are joined into each vehicle object here (see RedisStateService for
 * why crowding lives under its own keys).
 */
@Service
public class SnapshotService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RedisStateService redis;

    public SnapshotService(RedisStateService redis) {
        this.redis = redis;
    }

    public ObjectNode buildSnapshot() {
        ObjectNode snapshot = MAPPER.createObjectNode();
        snapshot.put("type", "snapshot");
        snapshot.set("vehicles", vehiclesWithCrowding());
        snapshot.set("delays", parseAll("delay:"));
        snapshot.set("alerts", parseAll("alert:"));
        snapshot.set("ripples", parseAll("ripple:"));
        return snapshot;
    }

    private ArrayNode vehiclesWithCrowding() {
        ArrayNode vehicles = MAPPER.createArrayNode();
        Map<String, String> crowding = redis.crowdingByVehicleId();
        for (String json : redis.valuesByPrefix("vehicle:")) {
            try {
                ObjectNode vehicle = (ObjectNode) MAPPER.readTree(json);
                String crowdingJson = crowding.get(vehicle.path("vehicle_id").asText());
                if (crowdingJson != null) {
                    JsonNode level = MAPPER.readTree(crowdingJson).path("crowding_level");
                    if (!level.isMissingNode()) {
                        vehicle.set("crowding_level", level);
                    }
                }
                vehicles.add(vehicle);
            } catch (Exception e) {
                log.warn("skipping unparseable vehicle entry: {}", e.getMessage());
            }
        }
        return vehicles;
    }

    private ArrayNode parseAll(String prefix) {
        ArrayNode array = MAPPER.createArrayNode();
        for (String json : redis.valuesByPrefix(prefix)) {
            try {
                array.add(MAPPER.readTree(json));
            } catch (Exception e) {
                log.warn("skipping unparseable {} entry: {}", prefix, e.getMessage());
            }
        }
        return array;
    }
}
