package io.joshworks.fstore.client;

import io.joshworks.fstore.client.tcp.Response;
import io.joshworks.fstore.client.tcp.TcpClient;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.tcp.EventsData;
import io.joshworks.fstore.es.shared.tcp.SubscriptionIteratorNext;

import java.util.ArrayDeque;
import java.util.Queue;

public class NodeIterator implements ClientStreamIterator {

    private final String subscriptionId;
    private final TcpClient client;
    private final int fetchSize;
    private final Queue<EventRecord> queue = new ArrayDeque<>();

    public NodeIterator(String subscriptionId, TcpClient client, int fetchSize) {
        this.subscriptionId = subscriptionId;
        this.client = client;
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
        Response<EventsData> received = client.send(new SubscriptionIteratorNext(subscriptionId, fetchSize));
        queue.addAll(received.get().events);
    }

    @Override
    public EventMap checkpoint() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void close() {

    }

}
