package io.joshworks.eventry.server.subscription.polling;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class LocalPollingSubscription {

    private final Map<String, EventStoreIterator> localIterators = new ConcurrentHashMap<>();
    private final IEventStore localStore;

    public LocalPollingSubscription(IEventStore localStore) {
        this.localStore = localStore;
    }

    public String create(String pattern) {
        String subscriptionId = UUID.randomUUID().toString().substring(0, 8);
        EventStoreIterator iterator = localStore.fromStreams(EventMap.empty(), Set.of(pattern));
        localIterators.put(subscriptionId, iterator);
        return subscriptionId;
    }

    public List<EventRecord> next(String subscriptionId, int batchSize) {
        EventStoreIterator it = localIterators.get(subscriptionId);
        if (it == null) {
            throw new IllegalArgumentException("No subscription found for id: " + subscriptionId);
        }

        List<EventRecord> records = new ArrayList<>();
        while (it.hasNext() && records.size() < batchSize) {
            records.add(it.next());
        }
        return records;
    }


}
