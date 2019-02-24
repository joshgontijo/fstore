package io.joshworks.fstore.core.io;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class StorageBase implements Storage {

    protected final AtomicLong position = new AtomicLong();
    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final String name;
    protected final long size;

    protected StorageBase(String name, long size) {
        this.name = name;
        this.size = size;
    }
}
