package io.joshworks.eventry.server.cluster.node;

import io.joshworks.eventry.api.IEventStore;

import java.io.Closeable;
import java.util.Objects;

public class Node implements Closeable {

    private final String id;
    private final IEventStore store;

    public Status status; //TODO use, lock etc..

    public Node(String id, IEventStore store) {
        this.id = id;
        this.store = store;
    }

    public IEventStore store() {
        return store;
    }

    public String id() {
        return id;
    }

    public Status status() {
        return status;
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
        return id == node.id &&
                Objects.equals(store, node.store) &&
                status == node.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, store, status);
    }
}
