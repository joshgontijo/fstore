package io.joshworks.eventry.server.tcp;

import io.joshworks.eventry.network.tcp.ServerEventHandler;
import io.joshworks.eventry.network.tcp.TcpConnection;
import io.joshworks.eventry.server.ClusterStore;
import io.joshworks.eventry.server.subscription.polling.LocalPollingSubscription;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.messages.Ack;
import io.joshworks.fstore.es.shared.messages.Append;
import io.joshworks.fstore.es.shared.messages.ClusterInfoRequest;
import io.joshworks.fstore.es.shared.messages.ClusterNodes;
import io.joshworks.fstore.es.shared.messages.CreateStream;
import io.joshworks.fstore.es.shared.messages.CreateSubscription;
import io.joshworks.fstore.es.shared.messages.ErrorMessage;
import io.joshworks.fstore.es.shared.messages.EventCreated;
import io.joshworks.fstore.es.shared.messages.EventData;
import io.joshworks.fstore.es.shared.messages.EventsData;
import io.joshworks.fstore.es.shared.messages.GetEvent;
import io.joshworks.fstore.es.shared.messages.SubscriptionCreated;
import io.joshworks.fstore.es.shared.messages.SubscriptionIteratorNext;
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

        handlers.add(ClusterInfoRequest.class, this::clusterNodes);
        handlers.add(CreateStream.class, this::createStream);
        handlers.add(Append.class, this::append);
        handlers.add(GetEvent.class, this::getEvent);
        handlers.add(CreateSubscription.class, this::createSubscription);
        handlers.add(SubscriptionIteratorNext.class, this::subscriptionIteratorNext);
    }

    @Override
    public Object onRequest(TcpConnection connection, Object data) {
        return handlers.handle(data, connection);
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

    private ClusterNodes clusterNodes(TcpConnection connection, ClusterInfoRequest msg) {
        return new ClusterNodes(store.nodesInfo());
    }

    private EventsData subscriptionIteratorNext(TcpConnection connection, SubscriptionIteratorNext msg) {
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
