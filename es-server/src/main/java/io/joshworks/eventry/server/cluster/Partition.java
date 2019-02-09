package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.IEventStore;

import java.io.Closeable;
import java.io.File;

public class Partition implements Closeable {

    public final int id;
    private boolean master;
    private final File root;
    private final IEventStore owner;

    public Partition(int id, File root, IEventStore owner) {
        this.id = id;
        this.root = root;
        this.owner = owner;
    }

    public IEventStore store() {
        return owner;
    }

    public File root() {
        return root;
    }

    @Override
    public void close() {
        owner.close();
    }
}
