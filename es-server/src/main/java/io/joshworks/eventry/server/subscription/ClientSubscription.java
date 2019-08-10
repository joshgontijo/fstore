package io.joshworks.eventry.server.subscription;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.subscription.SubscriptionOptions;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import io.joshworks.snappy.sse.SseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class ClientSubscription implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ClientSubscription.class);

    private final String subscriptionId;
    private final SubscriptionOptions options;
    private final EventStoreIterator iterator;
    private final SseContext context;
    private final long since;
    private final int maxItemsPerRound;
    private final AtomicLong itemsSent = new AtomicLong();

    private final AtomicBoolean active = new AtomicBoolean(true);

    private long lastSent;

    ClientSubscription(String subscriptionId, SubscriptionOptions options, EventStoreIterator iterator, SseContext context, int maxItemsPerRound) {
        this.subscriptionId = subscriptionId;
        this.options = options;
        this.iterator = iterator;
        this.context = context;
        this.since = System.currentTimeMillis();
        this.maxItemsPerRound = maxItemsPerRound;
    }

    public int send() {
        int sent = 0;
        try {
            List<EventRecord.JsonView> batch = new ArrayList<>(options.batchSize);
            while (active.get() && sent < maxItemsPerRound && iterator.hasNext()) {
                EventRecord event = iterator.next();
                if (!context.isOpen()) {
                    close();
                    return sent;
                }

                batch.add(event.asJson());
                if (batch.size() >= options.batchSize) {
                    sent += sendBatchItems(batch);
                    batch = new ArrayList<>();
                }
            }
            if (batch.size() > 0) {
                sent += sendBatchItems(batch);
            }
        } catch (Exception e) {
            logger.error("Failed to push event to subscription " + subscriptionId + ", closing connection", e);
            close();
        }
        return sent;
    }

    private int sendBatchItems(List<EventRecord.JsonView> batch) {
        context.send(JsonSerializer.toJson(batch));
        itemsSent.addAndGet(batch.size());
        lastSent = System.currentTimeMillis();
        return batch.size();
    }

    public String subscriptionId() {
        return subscriptionId;
    }

    public SubscriptionInfo info() {
        return new SubscriptionInfo(subscriptionId, since, itemsSent.get(), active.get(), context.isOpen(), lastSent);
    }

    public long since() {
        return since;
    }

    public AtomicLong itemsSent() {
        return itemsSent;
    }

    public AtomicBoolean active() {
        return active;
    }

    public long lastSent() {
        return lastSent;
    }

    boolean isOpen() {
        return context.isOpen();
    }

    void disable() {
        active.set(false);
    }

    void enable() {
        active.set(true);
    }

    @Override
    public void close() {
        context.close();
        IOUtils.closeQuietly(iterator);
    }

    void forceClose() {
        context.forceClose();
    }
}
