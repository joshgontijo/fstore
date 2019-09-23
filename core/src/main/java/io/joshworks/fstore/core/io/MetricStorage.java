package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.metrics.Metrics;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MetricStorage implements Storage {

    private final Metrics metrics = new Metrics();

    private final Storage delegate;

    public MetricStorage(Storage delegate) {
        this.delegate = delegate;
    }

    @Override
    public int write(ByteBuffer src) {
        long start = System.currentTimeMillis();
        int written = delegate.write(src);
        metrics.update("writeTime", (System.currentTimeMillis() - start));
        metrics.update("writes");
        metrics.update("bytesWritten", written);
        return written;
    }

    @Override
    public int write(long position, ByteBuffer src) {
        long start = System.currentTimeMillis();
        int written = delegate.write(position, src);
        metrics.update("writeTime", (System.currentTimeMillis() - start));
        metrics.update("writes");
        metrics.update("bytesWritten", written);
        return written;
    }

    @Override
    public long write(ByteBuffer[] srcs) {
        long start = System.currentTimeMillis();
        long written = delegate.write(srcs);
        metrics.update("writeTime", (System.currentTimeMillis() - start));
        metrics.update("writes");
        metrics.update("bytesWritten", written);
        return written;
    }

    @Override
    public int read(long position, ByteBuffer dst) {
        long start = System.currentTimeMillis();
        int read = delegate.read(position, dst);
        metrics.update("readTime", (System.currentTimeMillis() - start));
        metrics.update("read");
        metrics.update("bytesRead", read);
        return read;
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
    public void truncate(long newSize) {
        delegate.truncate(newSize);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    public Metrics metrics() {
        return metrics;
    }
}
