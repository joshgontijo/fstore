package io.joshworks.lsm.server.handler;

import io.joshworks.eventry.network.tcp.ServerEventHandler;
import io.joshworks.eventry.network.tcp.TcpConnection;
import io.joshworks.lsm.server.LsmCluster;
import io.joshworks.lsm.server.messages.Ack;
import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Get;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class TcpEventHandler implements ServerEventHandler {

    private final Handlers handlers = new Handlers();
    private final LsmCluster lsmtree;

    public TcpEventHandler(LsmCluster lsmtree) {
        this.lsmtree = lsmtree;

        handlers.add(Put.class, this::put);
        handlers.add(Get.class, this::get);
        handlers.add(Delete.class, this::delete);
    }

    @Override
    public Object onRequest(TcpConnection connection, Object data) {
        return handlers.handle(data, connection);
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        handlers.handle(data, connection);
    }

    private Ack put(TcpConnection connection, Put msg) {
        lsmtree.put(msg);
        return new Ack();
    }

    private Result get(TcpConnection connection, Get msg) {
        return lsmtree.get(msg);
    }

    private Ack delete(TcpConnection connection, Delete msg) {
        lsmtree.delete(msg);
        return new Ack();
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
                return new Error(e.getMessage());
            }
        }
    }
}
