package com.ttcintelligence.rippledetector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC helper for TimescaleDB lookups, backed by a small HikariCP pool.
 * Connection settings come from the DB_HOST, DB_PORT, DB_NAME, DB_USER and
 * DB_PASSWORD environment variables.
 */
public class TimescaleDBClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TimescaleDBClient.class);

    private static final String FEEDERS_QUERY =
            "SELECT station_id, station_name, route_id, distance_meters "
                    + "FROM station_feeder_routes WHERE station_id = ANY (?)";

    private final HikariDataSource dataSource;

    public TimescaleDBClient() {
        String host = env("DB_HOST", "localhost");
        String port = env("DB_PORT", "5432");
        String name = env("DB_NAME", "ttc_intelligence");

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + host + ":" + port + "/" + name);
        config.setUsername(env("DB_USER", "ttc"));
        config.setPassword(env("DB_PASSWORD", "ttc"));
        config.setMaximumPoolSize(2);
        config.setPoolName("timescaledb");
        this.dataSource = new HikariDataSource(config);
        log.info("TimescaleDB pool created for jdbc:postgresql://{}:{}/{}", host, port, name);
    }

    /**
     * Returns every feeder-route row for the given station ids. Empty result
     * simply means none of the ids are known subway stations.
     */
    public List<FeederRoute> getFeedersForStations(Collection<String> stationIds) {
        List<FeederRoute> feeders = new ArrayList<>();
        if (stationIds.isEmpty()) {
            return feeders;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(FEEDERS_QUERY)) {
            stmt.setArray(1, conn.createArrayOf("text", stationIds.toArray()));
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    feeders.add(new FeederRoute(
                            rs.getString(1), rs.getString(2), rs.getString(3), rs.getDouble(4)));
                }
            }
        } catch (SQLException e) {
            log.warn("feeder lookup failed for stations {}: {}", stationIds, e.getMessage());
        }
        return feeders;
    }

    @Override
    public void close() {
        dataSource.close();
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value != null && !value.isEmpty() ? value : fallback;
    }
}
