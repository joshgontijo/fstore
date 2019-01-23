package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.IEventStore;

public class Partition {

    private final int id;
    private final IEventStore owner;

    public Partition(int id, IEventStore owner) {
        this.id = id;
        this.owner = owner;
    }

}
