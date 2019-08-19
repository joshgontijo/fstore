package io.joshworks.eventry.server.tcp;

import io.joshworks.eventry.network.tcp.EventHandler;
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
import io.joshworks.fstore.es.shared.tcp.Message;
import io.joshworks.fstore.es.shared.tcp.SubscriptionCreated;
import io.joshworks.fstore.es.shared.tcp.SubscriptionIteratorNext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class TcpEventHandler implements EventHandler {

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
    public void onEvent(TcpConnection connection, Object data) {
        if (data instanceof Message) {
            Message msg = (Message) data;
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
        private final Map<Class, BiConsumer<TcpConnection, Message>> handlers = new ConcurrentHashMap<>();
        private final BiConsumer<TcpConnection, Message> NO_OP = (conn, msg) -> logger.warn("No handler for {}", msg.getClass().getSimpleName());

        private <T extends Message> void add(Class<T> type, BiConsumer<TcpConnection, T> handler) {
            handlers.put(type, (BiConsumer<TcpConnection, Message>) handler);
        }

        private void handle(Message msg, TcpConnection conn) {
            handlers.getOrDefault(msg.getClass(), NO_OP).accept(conn, msg);
        }
    }

    private void createSubscription(TcpConnection connection, CreateSubscription msg) {
        try {
            String subscriptionId = subscription.create(msg.pattern);
            reply(new SubscriptionCreated(subscriptionId), msg, connection);
        } catch (Exception e) {
            replyError(e, msg, connection);
        }
    }

    private void subscriptionIteratorNext(TcpConnection connection, SubscriptionIteratorNext msg) {
        try {
            long start = System.currentTimeMillis();
            List<EventRecord> entries = subscription.next(msg.subscriptionId, msg.batchSize);
//            System.out.println("ITERATOR NEXT TOOK: " + (System.currentTimeMillis() - start) + " ENTRIES: " + entries.size());
            reply(new EventsData(entries), msg, connection);

        } catch (Exception e) {
            e.printStackTrace();
            replyError(e, msg, connection);
        }
    }

    private void getEvent(TcpConnection connection, GetEvent msg) {
        try {
            EventRecord event = store.get(msg.eventId);
            reply(new EventData(event), msg, connection);
        } catch (Exception e) {
            replyError(e, msg, connection);
        }
    }

    private void createStream(TcpConnection connection, CreateStream msg) {
        try {
            StreamMetadata metadata = store.createStream(msg.name, msg.maxCount, msg.maxAgeSec, msg.acl, msg.metadata);
            reply(new Ack(), msg, connection);
        } catch (Exception e) {
            replyError(e, msg, connection);
        }
    }

    private void append(TcpConnection connection, Append msg) {
        try {
            EventRecord created = store.append(msg.record, msg.expectedVersion);
            if (replyExpected(msg)) {
                EventCreated eventCreated = new EventCreated(created.timestamp, created.version);
                reply(eventCreated, msg, connection);
            }
        } catch (Exception e) {
            if (replyExpected(msg)) {
                replyError(e, msg, connection);
            }
            logger.error(e.getMessage(), e);
        }
    }

    private boolean replyExpected(Message msg) {
        return msg.id != Message.NO_RESP;
    }

    private <T extends Message> void reply(T reply, Message original, TcpConnection connection) {
        reply.id = original.id;
        connection.sendAndFlush(reply);
    }

    private <T extends Message> void replyError(Exception e, Message original, TcpConnection connection) {
        reply(new ErrorMessage(e.getMessage()), original, connection);
    }

}
