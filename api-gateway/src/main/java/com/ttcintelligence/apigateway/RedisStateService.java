package com.ttcintelligence.apigateway;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Live-state cache in Redis. Key layout:
 *
 * <pre>
 *   vehicle:{vehicle_id}  latest position JSON        TTL 120s (silent vehicles drop off the map)
 *   crowding:{vehicle_id} latest crowding JSON        TTL 600s (crowding windows close every 5 min)
 *   delay:{route_id}      latest delay-score JSON     no TTL (overwritten by the next window)
 *   alert:{alert_id}      latest service alert JSON   no TTL (persist until TTC resolves them)
 *   ripple:{ripple_id}    latest cascade alert JSON   TTL 30 min (ripples clear themselves)
 * </pre>
 *
 * Crowding is stored under its own key rather than read-modify-written into
 * the vehicle JSON: the two topics update concurrently, so merging in place
 * would race (last writer wins could drop a position or a crowding level) and
 * would couple crowding's lifetime to the vehicle key's 120s TTL. The join
 * happens where it is race-free: server-side when assembling snapshots, and
 * client-side for live crowding_update events.
 */
@Service
public class RedisStateService {

    static final Duration VEHICLE_TTL = Duration.ofSeconds(120);
    static final Duration CROWDING_TTL = Duration.ofSeconds(600);
    static final Duration RIPPLE_TTL = Duration.ofMinutes(30);

    private final StringRedisTemplate redis;

    public RedisStateService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public void putVehicle(String vehicleId, String json) {
        redis.opsForValue().set("vehicle:" + vehicleId, json, VEHICLE_TTL);
    }

    public void putCrowding(String vehicleId, String json) {
        redis.opsForValue().set("crowding:" + vehicleId, json, CROWDING_TTL);
    }

    public void putDelay(String routeId, String json) {
        redis.opsForValue().set("delay:" + routeId, json);
    }

    public void putAlert(String alertId, String json) {
        redis.opsForValue().set("alert:" + alertId, json);
    }

    public void putRipple(String rippleId, String json) {
        redis.opsForValue().set("ripple:" + rippleId, json, RIPPLE_TTL);
    }

    /** All values under a key prefix, via SCAN (never KEYS). */
    public List<String> valuesByPrefix(String prefix) {
        List<String> keys = new ArrayList<>();
        ScanOptions options = ScanOptions.scanOptions().match(prefix + "*").count(500).build();
        try (Cursor<String> cursor = redis.scan(options)) {
            cursor.forEachRemaining(keys::add);
        }
        if (keys.isEmpty()) {
            return List.of();
        }
        List<String> values = redis.opsForValue().multiGet(keys);
        return values == null ? List.of() : values.stream().filter(v -> v != null).toList();
    }

    /** vehicle_id -> crowding JSON, for the server-side snapshot join. */
    public Map<String, String> crowdingByVehicleId() {
        Map<String, String> result = new HashMap<>();
        ScanOptions options = ScanOptions.scanOptions().match("crowding:*").count(500).build();
        List<String> keys = new ArrayList<>();
        try (Cursor<String> cursor = redis.scan(options)) {
            cursor.forEachRemaining(keys::add);
        }
        if (keys.isEmpty()) {
            return result;
        }
        List<String> values = redis.opsForValue().multiGet(keys);
        for (int i = 0; i < keys.size(); i++) {
            String value = values == null ? null : values.get(i);
            if (value != null) {
                result.put(keys.get(i).substring("crowding:".length()), value);
            }
        }
        return result;
    }
}
