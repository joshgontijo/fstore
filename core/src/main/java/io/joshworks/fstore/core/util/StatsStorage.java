package io.joshworks.fstore.core.util;

import io.joshworks.fstore.core.io.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class StatsStorage implements Storage {

    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();
    private long lastReadTime;
    private long lastWriteTime;
    private long biggestEntry;

    private final Storage delegate;

    public StatsStorage(Storage delegate) {
        this.delegate = delegate;
    }

    public long reads() {
        return reads.get();
    }

    public long writes() {
        return writes.get();
    }

    @Override
    public int write(ByteBuffer data) {
        int remaining = data.remaining();
        if (remaining > biggestEntry) {
            biggestEntry = remaining;
        }
        writes.incrementAndGet();
        long start = System.currentTimeMillis();
        int write = delegate.write(data);
        long timeTaken = System.currentTimeMillis() - start;
        lastWriteTime = timeTaken;
        return write;
    }

    @Override
    public int read(long position, ByteBuffer data) {
        reads.incrementAndGet();
        long start = System.currentTimeMillis();
        int read = delegate.read(position, data);
        long timeTaken = System.currentTimeMillis() - start;
        lastReadTime = timeTaken;
        return read;
    }

    @Override
    public long length() {
        return delegate.length();
    }

    @Override
    public void writePosition(long position) {
        delegate.writePosition(position);
    }

    @Override
    public long writePosition() {
        return delegate.writePosition();
    }

    @Override
    public void delete() {
        delegate.delete();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }
}
