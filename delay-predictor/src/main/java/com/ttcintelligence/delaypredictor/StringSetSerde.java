package com.ttcintelligence.delaypredictor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * JSON-backed Serde for the per-window set of vehicle ids, used by the
 * windowed aggregation's state store and changelog topic.
 */
public class StringSetSerde implements Serde<Set<String>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<HashSet<String>> TYPE = new TypeReference<>() {
    };

    @Override
    public Serializer<Set<String>> serializer() {
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
    public Deserializer<Set<String>> deserializer() {
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
