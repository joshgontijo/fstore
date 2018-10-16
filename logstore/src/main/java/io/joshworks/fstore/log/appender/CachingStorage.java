package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class CachingStorage implements Storage {

    private final Storage delegate;
    private AtomicLong cacheUsage;

    public CachingStorage(Storage delegate, AtomicLong cacheUsage) {
        this.delegate = delegate;
        this.cacheUsage = cacheUsage;
    }

    @Override
    public int write(ByteBuffer data) {
        int remaining = data.remaining();
        cacheUsage.

        return delegate.write(data);
    }

    @Override
    public int read(long position, ByteBuffer data) {
        return delegate.read(position, data);
    }

    @Override
    public long length() {
        return delegate.length();
    }

    @Override
    public void position(long position) {
        delegate.position(position);
    }

    @Override
    public long position() {
        return delegate.position();
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
    public void truncate(long pos) {
        delegate.truncate(pos);
    }

    @Override
    public void markAsReadOnly() {
        delegate.markAsReadOnly();
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}