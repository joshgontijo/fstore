package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.IEventStore;

import java.io.Closeable;

public class Partition implements Closeable {

    public final int id;
    private boolean master;
    private final IEventStore owner;

    public Partition(int id, IEventStore owner) {
        this.id = id;
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
