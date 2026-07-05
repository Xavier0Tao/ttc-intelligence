package com.ttcintelligence.apigateway;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Single firehose endpoint: on connect the client gets a full state snapshot,
 * then every Kafka-sourced update is broadcast to all connected sessions.
 * Sessions are wrapped in ConcurrentWebSocketSessionDecorator so concurrent
 * Kafka listener threads can send safely; slow clients get dropped messages
 * rather than blocking the pipeline.
 */
@Component
public class LiveWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(LiveWebSocketHandler.class);

    private static final int SEND_TIME_LIMIT_MS = 2_000;
    private static final int SEND_BUFFER_LIMIT_BYTES = 1_024 * 1_024;

    private final Set<WebSocketSession> sessions = new CopyOnWriteArraySet<>();
    private final SnapshotService snapshotService;

    public LiveWebSocketHandler(SnapshotService snapshotService) {
        this.snapshotService = snapshotService;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession rawSession) throws IOException {
        WebSocketSession session = new ConcurrentWebSocketSessionDecorator(
                rawSession, SEND_TIME_LIMIT_MS, SEND_BUFFER_LIMIT_BYTES,
                ConcurrentWebSocketSessionDecorator.OverflowStrategy.DROP);
        session.sendMessage(new TextMessage(snapshotService.buildSnapshot().toString()));
        sessions.add(session);
        log.info("ws client connected ({} total)", sessions.size());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.removeIf(s -> s.getId().equals(session.getId()));
        log.info("ws client disconnected ({} total)", sessions.size());
    }

    public void broadcast(String type, String dataJson) {
        if (sessions.isEmpty()) {
            return;
        }
        TextMessage message = new TextMessage("{\"type\":\"" + type + "\",\"data\":" + dataJson + "}");
        for (WebSocketSession session : sessions) {
            try {
                if (session.isOpen()) {
                    session.sendMessage(message);
                } else {
                    sessions.remove(session);
                }
            } catch (Exception e) {
                log.warn("dropping ws session after send failure: {}", e.getMessage());
                sessions.remove(session);
            }
        }
    }
}
