package io.joshworks.eventry.server.subscription;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.core.util.Threads;
import io.undertow.server.handlers.sse.ServerSentEventConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Subscriptions implements Closeable {

    private static final Map<String, ClientSubscription> items = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(Subscriptions.class);

    private final ExecutorService executor;
    private final SubscriptionWorker[] workers;
    private final int maxItemsPerRound;

    private static final XXHash hash = new XXHash();

    public Subscriptions(int workers, int waitTime, int maxItemsPerRound) {
        this.maxItemsPerRound = maxItemsPerRound;
        this.executor = Executors.newFixedThreadPool(workers, Threads.namePrefixedThreadFactory("subscription-worker"));
        this.workers = new SubscriptionWorker[workers];
        for (int i = 0; i < workers; i++) {
            var worker = new SubscriptionWorker(waitTime);
            this.workers[i] = worker;
            this.executor.submit(worker);
        }
    }

    private SubscriptionWorker select(String subscriptionId) {
        int hash = Subscriptions.hash.hash32(subscriptionId.getBytes(StandardCharsets.UTF_8));
        int idx = Math.abs(hash) % workers.length;
        return workers[idx];
    }

    public boolean create(String subscriptionId, EventStoreIterator iterator, ServerSentEventConnection connection) {
        var client = new ClientSubscription(subscriptionId, iterator, connection, maxItemsPerRound);
        return select(subscriptionId).add(client);
    }

    public void remove(String subscriptionId) {
        select(subscriptionId).remove(subscriptionId);
    }

    public void disable(String subscriptionId) {
        select(subscriptionId).disable(subscriptionId);
    }

    public void enable(String subscriptionId) {
        select(subscriptionId).enable(subscriptionId);
    }

    @Override
    public void close() {
        for (SubscriptionWorker worker : workers) {
            worker.close();
        }
    }

}
