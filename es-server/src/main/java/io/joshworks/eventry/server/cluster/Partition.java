package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.IEventStore;

import java.io.Closeable;

public class Partition implements Closeable {

    public final int id;
    private boolean master;
    public final boolean local;
    private final IEventStore owner;

    public Partition(int id, boolean local, IEventStore owner) {
        this.id = id;
        this.local = local;
        this.owner = owner;
    }

    public IEventStore store() {
        return owner;
    }


    @Override
    public void close() {
        owner.close();
    }
}
