package io.joshworks.eventry.server.tcp;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import io.joshworks.eventry.server.ClusterStore;
import io.joshworks.eventry.server.subscription.polling.LocalPollingSubscription;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.tcp.Ack;
import io.joshworks.fstore.es.shared.tcp.Append;
import io.joshworks.fstore.es.shared.tcp.CreateStream;
import io.joshworks.fstore.es.shared.tcp.CreateSubscription;
import io.joshworks.fstore.es.shared.tcp.ErrorMessage;
import io.joshworks.fstore.es.shared.tcp.EventData;
import io.joshworks.fstore.es.shared.tcp.EventCreated;
import io.joshworks.fstore.es.shared.tcp.EventsData;
import io.joshworks.fstore.es.shared.tcp.GetEvent;
import io.joshworks.fstore.es.shared.tcp.Message;
import io.joshworks.fstore.es.shared.tcp.SubscriptionCreated;
import io.joshworks.fstore.es.shared.tcp.SubscriptionIteratorNext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

class TcpEventHandler extends Listener {

    private final ClusterStore store;
    private final LocalPollingSubscription subscription;

    private static final Logger logger = LoggerFactory.getLogger(TcpEventHandler.class);

    private final Handlers handlers = new Handlers();

    TcpEventHandler(ClusterStore store, LocalPollingSubscription subscription) {
        this.store = store;
        this.subscription = subscription;

        handlers.add(CreateStream.class, this::createStream);
        handlers.add(Append.class, this::append);
        handlers.add(GetEvent.class, this::getEvent);
        handlers.add(CreateSubscription.class, this::createSubscription);
        handlers.add(SubscriptionIteratorNext.class, this::subscriptionIteratorNext);
    }

    @Override
    public void connected(Connection connection) {
        System.out.println("Accepted connection from " + connection.getRemoteAddressTCP());
    }

    @Override
    public void disconnected(Connection connection) {
        System.out.println("Client disconnected: " + connection.getRemoteAddressTCP());
    }

    @Override
    public void received(Connection connection, Object object) {
        if (object instanceof Message) {
            Message msg = (Message) object;
            try {
                handlers.handle(msg, connection);
            } catch (Exception e) {
                logger.error("Error while handling message", e);
                if (replyExpected(msg)) {
                    reply(new ErrorMessage(e.getMessage()), msg, connection);
                } else {
                    logger.warn("Client will not know that the error occurred");
                }
            }
        }
    }

    private static class Handlers {

        private final Logger logger = LoggerFactory.getLogger(Handlers.class);
        private final Map<Class, BiConsumer<Connection, Message>> handlers = new ConcurrentHashMap<>();
        private final BiConsumer<Connection, Message> NO_OP = (conn, msg) -> logger.warn("No handler for {}", msg.getClass().getSimpleName());

        private <T extends Message> void add(Class<T> type, BiConsumer<Connection, T> handler) {
            handlers.put(type, (BiConsumer<Connection, Message>) handler);
        }

        private void handle(Message msg, Connection conn) {
            handlers.getOrDefault(msg.getClass(), NO_OP).accept(conn, msg);
        }
    }

    private void createSubscription(Connection connection, CreateSubscription msg) {
        String subscriptionId = subscription.create(msg.pattern);
        reply(new SubscriptionCreated(subscriptionId), msg, connection);
    }

    private void subscriptionIteratorNext(Connection connection, SubscriptionIteratorNext msg) {
        List<EventRecord> entries = subscription.next(msg.subscriptionId, msg.batchSize);
        reply(new EventsData(entries), msg, connection);
    }

    private void getEvent(Connection connection, GetEvent msg) {
        EventRecord event = store.get(msg.eventId);
        reply(new EventData(event), msg, connection);
    }

    private void createStream(Connection connection, CreateStream msg) {
        StreamMetadata metadata = store.createStream(msg.name, msg.maxCount, msg.maxAgeSec, msg.acl, msg.metadata);
        reply(new Ack(), msg, connection);
    }

    private void append(Connection connection, Append msg) {
        EventRecord created = store.append(msg.record, msg.expectedVersion);
        if (replyExpected(msg)) {
            EventCreated eventCreated = new EventCreated(created.timestamp, created.version);
            reply(eventCreated, msg, connection);
        }
    }

    private boolean replyExpected(Message msg) {
        return msg.id != Message.NO_RESP;
    }

    private <T extends Message> void reply(T reply, Message original, Connection connection) {
        reply.id = original.id;
        connection.sendTCP(reply);
    }

}
