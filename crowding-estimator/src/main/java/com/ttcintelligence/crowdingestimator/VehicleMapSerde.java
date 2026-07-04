package com.ttcintelligence.crowdingestimator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * JSON-backed Serde for the per-window map of vehicle_id to latest snapshot,
 * used by the windowed aggregation's state store and changelog topic.
 */
public class VehicleMapSerde implements Serde<Map<String, VehicleSnapshot>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<HashMap<String, VehicleSnapshot>> TYPE = new TypeReference<>() {
    };

    @Override
    public Serializer<Map<String, VehicleSnapshot>> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    @Override
    public Deserializer<Map<String, VehicleSnapshot>> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, TYPE);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
