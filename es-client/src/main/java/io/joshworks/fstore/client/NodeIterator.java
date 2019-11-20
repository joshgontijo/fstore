package io.joshworks.fstore.client;

import io.joshworks.fstore.tcp.TcpClientConnection;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.messages.EventsData;
import io.joshworks.fstore.es.shared.messages.SubscriptionClose;
import io.joshworks.fstore.es.shared.messages.SubscriptionIteratorNext;

import java.util.ArrayDeque;
import java.util.Queue;

public class NodeIterator implements ClientStreamIterator {

    private final String subscriptionId;
    private final TcpClientConnection conn;
    private final int fetchSize;
    private final Queue<EventRecord> queue = new ArrayDeque<>();

    //TODO add empty entries backoff

    public NodeIterator(String subscriptionId, TcpClientConnection conn, int fetchSize) {
        this.subscriptionId = subscriptionId;
        this.conn = conn;
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
        Response<EventsData> received = conn.request(new SubscriptionIteratorNext(subscriptionId, fetchSize));
        EventsData data = received.get();
        queue.addAll(data.events);
    }

    @Override
    public EventMap checkpoint() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void close() {
       conn.send(new SubscriptionClose(subscriptionId));
    }

}
