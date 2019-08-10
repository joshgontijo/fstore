package io.joshworks.eventry.server.subscription;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.es.shared.subscription.SubscriptionOptions;
import io.joshworks.snappy.sse.SseContext;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Subscriptions implements Closeable {

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

    public boolean create(String subscriptionId, SubscriptionOptions options, EventStoreIterator iterator, SseContext context) {
        var client = new ClientSubscription(subscriptionId, options, iterator, context, maxItemsPerRound);
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

    public List<SubscriptionInfo> info() {
        return Arrays.stream(workers).flatMap(w -> w.info().stream()).collect(Collectors.toList());
    }
}
