package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.IEventStore;

public class Partition {

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
}
