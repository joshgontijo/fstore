package io.joshworks.eventry.server.subscription;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.snappy.sse.EventData;
import io.undertow.server.handlers.sse.ServerSentEventConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class ClientSubscription implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ClientSubscription.class);

    private final String subscriptionId;
    private final EventStoreIterator iterator;
    private final ServerSentEventConnection connection;
    private final long since;
    final int maxItemsPerRound;
    private final AtomicLong itemsSent = new AtomicLong();

    private final AtomicBoolean active = new AtomicBoolean(true);

    private long lastSent;

    ClientSubscription(String subscriptionId, EventStoreIterator iterator, ServerSentEventConnection connection, int maxItemsPerRound) {
        this.subscriptionId = subscriptionId;
        this.iterator = iterator;
        this.connection = connection;
        this.since = System.currentTimeMillis();
        this.maxItemsPerRound = maxItemsPerRound;
    }

    public synchronized int send() {
        int sent = 0;
        try {
            while (active.get() && sent < maxItemsPerRound && iterator.hasNext()) {
                EventRecord event = iterator.next();
                String eventId = EventId.toString(event.stream, event.version);
                EventData eventData = new EventData(event.dataAsJson(), eventId, event.type);
                if (connection.isOpen()) {
                    connection.send(eventData.data, eventData.event, eventData.id, null);

                    itemsSent.incrementAndGet();
                    lastSent = System.currentTimeMillis();
                    sent++;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to push event to subscription " + subscriptionId + ", closing connection", e);
            close();
        }
        return sent;
    }

    public String subscriptionId() {
        return subscriptionId;
    }

    synchronized boolean isOpen() {
        return connection.isOpen();
    }

    synchronized void disable() {
        active.set(false);
    }

    synchronized void enable() {
        active.set(true);
    }

    @Override
    public synchronized void close() {
        IOUtils.closeQuietly(connection);
        IOUtils.closeQuietly(iterator);
    }
}
