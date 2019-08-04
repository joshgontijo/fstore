package io.joshworks.eventry.server.cluster.node;

import io.joshworks.eventry.api.IEventStore;

import java.io.Closeable;
import java.util.Objects;

public class Partition implements Closeable {

    private final String id;
    private final String nodeId;
    private boolean master;
    private final IEventStore store;

    public Status status; //TODO use, lock etc..

    public Partition(String id, String nodeId, IEventStore store) {
        this.id = id;
        this.nodeId = nodeId;
        this.store = store;
    }

    public boolean master() {
        return master;
    }

    public IEventStore store() {
        return store;
    }

    public String id() {
        return id;
    }

    public String nodeId() {
        return nodeId;
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
        Partition partition = (Partition) o;
        return id == partition.id &&
                master == partition.master &&
                Objects.equals(store, partition.store) &&
                status == partition.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, master, store, status);
    }
}
