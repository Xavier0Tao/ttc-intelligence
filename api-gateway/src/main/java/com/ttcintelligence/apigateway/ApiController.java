package com.ttcintelligence.apigateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@CrossOrigin
public class ApiController {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JdbcTemplate jdbc;
    private final SnapshotService snapshotService;
    private final RedisStateService redis;

    public ApiController(JdbcTemplate jdbc, SnapshotService snapshotService, RedisStateService redis) {
        this.jdbc = jdbc;
        this.snapshotService = snapshotService;
        this.redis = redis;
    }

    @GetMapping("/routes")
    public List<Map<String, Object>> routes() {
        return jdbc.queryForList(
                "SELECT route_id, route_short_name, route_type FROM routes ORDER BY route_short_name");
    }

    /**
     * routeId is the route short name (e.g. "504"), consistent with delays.
     * Returns the dominant-shape path for one direction (0 by default) — a
     * single geographically continuous stop sequence, which is what the
     * dashboard's hover overlay needs. Falls back to direction 1 for routes
     * that only run in one direction.
     */
    @GetMapping("/routes/{routeId}/stops")
    public List<Map<String, Object>> routeStops(@PathVariable String routeId,
            @RequestParam(name = "direction", defaultValue = "0") int direction) {
        List<Map<String, Object>> stops = queryStops(routeId, direction);
        if (stops.isEmpty() && direction == 0) {
            stops = queryStops(routeId, 1);
        }
        return stops;
    }

    private List<Map<String, Object>> queryStops(String routeId, int direction) {
        return jdbc.queryForList(
                "SELECT stop_id, stop_name, latitude, longitude, stop_sequence "
                        + "FROM route_stops WHERE route_id = ? AND direction_id = ? "
                        + "ORDER BY stop_sequence",
                routeId, direction);
    }

    @GetMapping("/snapshot")
    public ObjectNode snapshot() {
        return snapshotService.buildSnapshot();
    }

    @GetMapping("/alerts/active")
    public ArrayNode activeAlerts() {
        ArrayNode array = MAPPER.createArrayNode();
        for (String prefix : new String[]{"alert:", "ripple:"}) {
            for (String json : redis.valuesByPrefix(prefix)) {
                try {
                    array.add(MAPPER.readTree(json));
                } catch (Exception ignored) {
                    // skip unparseable entries
                }
            }
        }
        return array;
    }
}
