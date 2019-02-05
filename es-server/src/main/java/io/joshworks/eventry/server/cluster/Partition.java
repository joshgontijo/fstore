package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.IEventStore;

import java.io.Closeable;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;

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


    public void transferTo(OutputStream out) {
        try {
            close();
            Files.copy(root.toPath(), out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        owner.close();
    }
}
