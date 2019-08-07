package io.joshworks.eventry.server.cluster.node;

import io.joshworks.eventry.api.IEventStore;
import io.joshworks.fstore.es.shared.Status;

import java.io.Closeable;
import java.util.Objects;

public class Node implements Closeable {

    public final String id;
    public final String address;
    public Status status = Status.ACTIVE; //TODO use, lock etc..

    private final IEventStore store;

    public Node(String id, IEventStore store, String address) {
        this.id = id;
        this.store = store;
        this.address = address;
    }

    public IEventStore store() {
        return store;
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return id.equals(node.id) &&
                store.equals(node.store) &&
                status == node.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, store, status);
    }
}
