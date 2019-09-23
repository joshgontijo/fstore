package io.joshworks.fstore.log.server.tcp;

import io.joshworks.eventry.network.tcp.ServerEventHandler;
import io.joshworks.eventry.network.tcp.TcpConnection;
import io.joshworks.eventry.network.tcp.internal.ErrorMessage;
import io.joshworks.fstore.log.server.PartitionedLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class TcpEventHandler implements ServerEventHandler {

    private final PartitionedLog store;
    private final Handlers handlers = new Handlers();
    private static final Ack ACK = new Ack();

    public TcpEventHandler(PartitionedLog store) {
        this.store = store;

        handlers.add(Append.class, this::append);
    }

    @Override
    public Object onRequest(TcpConnection connection, Object data) {
        return handlers.handle(data, connection);
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        handlers.handle(data, connection);
    }

    private static class Handlers {

        private final Logger logger = LoggerFactory.getLogger(Handlers.class);
        private final Map<Class, BiFunction<TcpConnection, Object, Object>> handlers = new ConcurrentHashMap<>();
        private final BiFunction<TcpConnection, Object, Object> NO_OP = (conn, msg) -> {
            logger.warn("No handler for {}", msg.getClass().getSimpleName());
            return null;
        };

        private <T> void add(Class<T> type, BiConsumer<TcpConnection, T> handler) {
            add(type, (tcpConnection, t) -> {
                handler.accept(tcpConnection, t);
                return null;
            });
        }

        private <T> void add(Class<T> type, BiFunction<TcpConnection, T, Object> handler) {
            handlers.put(type, (BiFunction<TcpConnection, Object, Object>) handler);
        }

        private Object handle(Object msg, TcpConnection conn) {
            try {
                return handlers.getOrDefault(msg.getClass(), NO_OP).apply(conn, msg);
            } catch (Exception e) {
                logger.error("Error handling event " + msg.getClass().getSimpleName(), e);
                return new ErrorMessage(e.getMessage());
            }
        }
    }

    private Ack append(TcpConnection connection, Append msg) {
        store.append(msg.key, msg.record);
        return ACK;
    }
}
