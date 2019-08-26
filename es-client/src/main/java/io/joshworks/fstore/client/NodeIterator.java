package io.joshworks.fstore.client;

import io.joshworks.eventry.network.tcp.internal.Response;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.tcp.EventsData;
import io.joshworks.fstore.es.shared.tcp.SubscriptionIteratorNext;

import java.util.ArrayDeque;
import java.util.Queue;

public class NodeIterator implements ClientStreamIterator {

    private final String subscriptionId;
    private final Node node;
    private final int fetchSize;
    private final Queue<EventRecord> queue = new ArrayDeque<>();

    //TODO add empty entries backoff

    public NodeIterator(String subscriptionId, Node node, int fetchSize) {
        this.subscriptionId = subscriptionId;
        this.node = node;
        this.fetchSize = fetchSize;
    }

    @Override
    public boolean hasNext() {
        if (!queue.isEmpty()) {
            return true;
        }
        fetch();
        return !queue.isEmpty();
    }

    @Override
    public EventRecord next() {
        if (!hasNext()) {
            return null;
        }
        return queue.poll();
    }

    private void fetch() {
        Response<EventsData> received = node.client().request(new SubscriptionIteratorNext(subscriptionId, fetchSize));
        EventsData data = received.get();
        queue.addAll(data.events);
    }

    @Override
    public EventMap checkpoint() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void close() {

    }

}
