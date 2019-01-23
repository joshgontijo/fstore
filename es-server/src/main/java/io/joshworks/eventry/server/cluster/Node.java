package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.IEventStore;

public class Node {

    private final IEventStore store;

    public Node(IEventStore store) {
        this.store = store;
    }
}
