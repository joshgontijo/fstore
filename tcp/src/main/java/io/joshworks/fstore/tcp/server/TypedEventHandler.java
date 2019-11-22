package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.TcpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class TypedEventHandler implements ServerEventHandler {

    private final Logger logger = LoggerFactory.getLogger(TypedEventHandler.class);
    private final Map<Class, BiFunction<TcpConnection, Object, Object>> handlers = new ConcurrentHashMap<>();
    private final BiFunction<TcpConnection, Object, Object> NO_OP = (conn, msg) -> {
        logger.warn("No handler for {}", msg.getClass().getSimpleName());
        return null;
    };

    @Override
    public Object onEvent(TcpConnection connection, Object data) {
        return handle(data, connection);
    }

    public <T> TypedEventHandler on(Class<T> type, BiConsumer<TcpConnection, T> handler) {
        on(type, (tcpConnection, t) -> {
            handler.accept(tcpConnection, t);
            return null;
        });
        return this;
    }

    public <T> TypedEventHandler on(Class<T> type, BiFunction<TcpConnection, T, Object> handler) {
        handlers.put(type, (BiFunction<TcpConnection, Object, Object>) handler);
        return this;
    }

    private Object handle(Object msg, TcpConnection conn) {
        try {
            return handlers.getOrDefault(msg.getClass(), NO_OP).apply(conn, msg);
        } catch (Exception e) {
            logger.error("Error handling event " + msg.getClass().getSimpleName(), e);
            return new Error(e.getMessage());
        }
    }

}
