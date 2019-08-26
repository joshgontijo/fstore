package io.joshworks.eventry.server.tcp;

import io.joshworks.eventry.network.tcp.ServerEventHandler;
import io.joshworks.eventry.network.tcp.TcpConnection;
import io.joshworks.eventry.server.ClusterStore;
import io.joshworks.eventry.server.subscription.polling.LocalPollingSubscription;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.tcp.Ack;
import io.joshworks.fstore.es.shared.tcp.Append;
import io.joshworks.fstore.es.shared.tcp.CreateStream;
import io.joshworks.fstore.es.shared.tcp.CreateSubscription;
import io.joshworks.fstore.es.shared.tcp.ErrorMessage;
import io.joshworks.fstore.es.shared.tcp.EventCreated;
import io.joshworks.fstore.es.shared.tcp.EventData;
import io.joshworks.fstore.es.shared.tcp.EventsData;
import io.joshworks.fstore.es.shared.tcp.GetEvent;
import io.joshworks.fstore.es.shared.tcp.SubscriptionCreated;
import io.joshworks.fstore.es.shared.tcp.SubscriptionIteratorNext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class TcpEventHandler implements ServerEventHandler {

    private final ClusterStore store;
    private final LocalPollingSubscription subscription;

    private static final Logger logger = LoggerFactory.getLogger(TcpEventHandler.class);

    private final Handlers handlers = new Handlers();

    public TcpEventHandler(ClusterStore store, LocalPollingSubscription subscription) {
        this.store = store;
        this.subscription = subscription;

        handlers.add(CreateStream.class, this::createStream);
        handlers.add(Append.class, this::append);
        handlers.add(GetEvent.class, this::getEvent);
        handlers.add(CreateSubscription.class, this::createSubscription);
        handlers.add(SubscriptionIteratorNext.class, this::subscriptionIteratorNext);
    }

    @Override
    public Object onRequest(TcpConnection connection, Object data) {
        return handlers.handle(data.getClass(), connection);
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        if (data instanceof Append) {
            //response ignored, this was an appendAsync from the client
            handlers.handle(data, connection);
        }
    }

    private static class Handlers {

        private final Logger logger = LoggerFactory.getLogger(Handlers.class);
        private final Map<Class, BiFunction<TcpConnection, Object, Object>> handlers = new ConcurrentHashMap<>();
        private final BiFunction<TcpConnection, Object, Object> NO_OP = (conn, msg) -> {
            logger.warn("No handler for {}", msg.getClass().getSimpleName());
            return null;
        };

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

    private SubscriptionCreated createSubscription(TcpConnection connection, CreateSubscription msg) {
        String subscriptionId = subscription.create(msg.pattern);
        return new SubscriptionCreated(subscriptionId);
    }

    private Object subscriptionIteratorNext(TcpConnection connection, SubscriptionIteratorNext msg) {
        List<EventRecord> entries = subscription.next(msg.subscriptionId, msg.batchSize);
        return new EventsData(entries);
    }

    private EventData getEvent(TcpConnection connection, GetEvent msg) {
        EventRecord event = store.get(msg.eventId);
        return new EventData(event);
    }

    private Ack createStream(TcpConnection connection, CreateStream msg) {
        StreamMetadata metadata = store.createStream(msg.name, msg.maxCount, msg.maxAgeSec, msg.acl, msg.metadata);
        return new Ack();
    }

    private EventCreated append(TcpConnection connection, Append msg) {
        EventRecord created = store.append(msg.record, msg.expectedVersion);
        return new EventCreated(created.timestamp, created.version);
    }
}
