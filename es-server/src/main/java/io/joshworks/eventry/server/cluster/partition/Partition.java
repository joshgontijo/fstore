package io.joshworks.eventry.server.cluster.partition;

import io.joshworks.eventry.IEventStore;

import java.io.Closeable;

public class Partition implements Closeable {

    public final int id;
    private boolean master;
    private String nodeId;
    private final IEventStore owner;
    public Status status; //TODO use, lock etc..

    public Partition(int id, IEventStore owner) {
        this.id = id;
        this.owner = owner;
    }

    public boolean master() {
        return master;
    }

    public IEventStore store() {
        return owner;
    }

    public void updateNode(String newNodeId) {
        this.nodeId = newNodeId;
    }

    public String nodeId() {
        return nodeId;
    }

    @Override
    public void close() {
        owner.close();
    }
}
