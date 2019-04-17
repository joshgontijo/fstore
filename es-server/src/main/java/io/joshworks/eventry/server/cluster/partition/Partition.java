package io.joshworks.eventry.server.cluster.partition;

import io.joshworks.eventry.IEventStore;

import java.io.Closeable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Partition implements Closeable {

    public final int id;
    private boolean master;
    private final String owner;
    private final IEventStore store;
    private final Set<String> isr = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> replicas = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public Status status; //TODO use, lock etc..

    public Partition(int id, String owner, IEventStore store) {
        this.id = id;
        this.owner = owner;
        this.store = store;
    }

    public boolean master() {
        return master;
    }

    public IEventStore store() {
        return store;
    }

    public String owner() {
        return owner;
    }

    public Set<String> replicas() {
        return Collections.unmodifiableSet(replicas);
    }

    @Override
    public void close() {
        store.close();
    }

    public boolean ownedBy(String nodeId) {
        return owner.equals(nodeId);
    }

    public boolean replicatedBy(String nodeId) {
        return replicas.contains(nodeId);
    }

}