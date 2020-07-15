package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class SegmentChannel extends FileChannel {

    private final File file;
    private final FileChannel delegate;
    private final AtomicLong writePosition = new AtomicLong();
    private final AtomicBoolean readOnly = new AtomicBoolean();

    private SegmentChannel(File file, FileChannel delegate) {
        this.file = file;
        this.delegate = delegate;
    }

    public static SegmentChannel open(File file) {
        try {
            boolean newFile = FileUtils.createIfNotExists(file);
            FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            SegmentChannel segmentChannel = new SegmentChannel(file, channel);
            if (!newFile) {
                seekEndOfLog(segmentChannel);
            }
            segmentChannel.readOnly.set(!newFile);

            return segmentChannel;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment", e);
        }
    }

    private static void seekEndOfLog(FileChannel channel) {
        try {
            channel.position(channel.size());
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to set position at the of the log", e);
        }
    }

    private long update(long position, long written) {
        if (written > 0) {
            writePosition.accumulateAndGet(written, (curr, add) -> Math.max(position + written, curr));
        }
        return written;
    }

    private void checkReadOnly() {
        if (readOnly()) {
            throw new IllegalStateException("Segment " + name() + "is read only");
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return delegate.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return delegate.read(dsts, offset, length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        checkReadOnly();
        return (int) update(writePosition.get(), delegate.write(src));
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        checkReadOnly();
        long written = delegate.write(srcs, offset, length);
        return update(writePosition.get(), written);
    }

    @Override
    public long position() {
        return writePosition.get();
    }

    @Override
    public FileChannel position(long newPosition) {
        checkReadOnly();
        try {
            writePosition.set(newPosition);
            return delegate.position(newPosition);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to set position", e);
        }
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public FileChannel truncate(long size) {
        try {
            //sets the writePosition to the truncated size if greater than truncated size
            writePosition.accumulateAndGet(size, Math::min);
            delegate.truncate(size);
            return this;
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to truncate file");
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        delegate.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return delegate.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        checkReadOnly();
        return delegate.transferFrom(src, position, count);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return delegate.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        checkReadOnly();
        return delegate.write(src, position);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new UnsupportedOperationException("Cannot mmap segment");
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return delegate.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        delegate.close();
    }

    void truncate() {
        try {
            long pos = writePosition.get();
            delegate.truncate(pos);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to truncate file");
        }
    }

    void delete() {
        try {
            delegate.close();
            Files.delete(file.toPath());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public String name() {
        return file.getName();
    }

    boolean markAsReadOnly() {
        return readOnly.compareAndSet(false, true);
    }

    boolean unmarkAsReadOnly() {
        return readOnly.compareAndSet(true, false);
    }

    boolean readOnly() {
        return readOnly.get();
    }
}
