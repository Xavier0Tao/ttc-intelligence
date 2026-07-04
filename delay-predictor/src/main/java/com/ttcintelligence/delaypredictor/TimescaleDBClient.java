package com.ttcintelligence.delaypredictor;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.OptionalDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC helper for the scheduled_headways table in TimescaleDB, backed by a
 * small HikariCP connection pool. Connection settings come from the
 * DB_HOST, DB_PORT, DB_NAME, DB_USER and DB_PASSWORD environment variables.
 */
public class TimescaleDBClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TimescaleDBClient.class);

    private static final String HEADWAY_QUERY =
            "SELECT scheduled_headway_minutes FROM scheduled_headways WHERE route_id = ? AND hour_of_day = ?";

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
     * Returns the scheduled headway in minutes for a route at the given hour
     * of day, or empty if no schedule entry exists (or the query fails).
     */
    public OptionalDouble getScheduledHeadway(String routeId, int hourOfDay) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(HEADWAY_QUERY)) {
            stmt.setString(1, routeId);
            stmt.setInt(2, hourOfDay);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return OptionalDouble.of(rs.getDouble(1));
                }
                return OptionalDouble.empty();
            }
        } catch (SQLException e) {
            log.warn("scheduled headway lookup failed for route={} hour={}: {}", routeId, hourOfDay, e.getMessage());
            return OptionalDouble.empty();
        }
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
