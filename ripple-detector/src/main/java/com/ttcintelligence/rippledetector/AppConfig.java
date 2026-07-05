package com.ttcintelligence.rippledetector;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads application.properties from the classpath and resolves
 * {@code ${ENV_VAR:default}} placeholders against the process environment.
 */
public final class AppConfig {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^:}]+)(?::([^}]*))?}");

    private AppConfig() {
    }

    public static Properties load() {
        Properties props = new Properties();
        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (in == null) {
                throw new IllegalStateException("application.properties not found on classpath");
            }
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read application.properties", e);
        }

        Properties resolved = new Properties();
        for (String name : props.stringPropertyNames()) {
            resolved.setProperty(name, resolve(props.getProperty(name)));
        }
        return resolved;
    }

    static String resolve(String raw) {
        Matcher matcher = PLACEHOLDER.matcher(raw);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            String env = System.getenv(matcher.group(1));
            String fallback = matcher.group(2) == null ? "" : matcher.group(2);
            matcher.appendReplacement(out, Matcher.quoteReplacement(env != null ? env : fallback));
        }
        matcher.appendTail(out);
        return out.toString();
    }
}
