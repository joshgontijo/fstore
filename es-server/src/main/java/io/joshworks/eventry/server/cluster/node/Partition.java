package io.joshworks.eventry.server.cluster.node;

import io.joshworks.eventry.api.IEventStore;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Objects;

public class Partition implements Closeable {

    public final int id;
    private boolean master;
    private final IEventStore store;
    private final int[] buckets;

    public Status status; //TODO use, lock etc..

    public Partition(int id, int[] buckets, IEventStore store) {
        this.id = id;
        this.buckets = buckets;
        this.store = store;
    }

    public boolean master() {
        return master;
    }

    public IEventStore store() {
        return store;
    }

    public int[] buckets() {
        return Arrays.copyOf(buckets, buckets.length);
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
