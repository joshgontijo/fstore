package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

public abstract class MemStorage implements Storage {

    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    private final int bufferSize;
    protected final AtomicLong position = new AtomicLong();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final String name;
    protected final long size;

    private final ByteBuffer[] buffers;

    protected MemStorage(String name, long size, BiFunction<Long, Integer, ByteBuffer> supplier) {
        this(name, size, MAX_BUFFER_SIZE, supplier);
    }

    protected MemStorage(String name, long size, int bufferSize, BiFunction<Long, Integer, ByteBuffer> supplier) {
        this.name = name;
        this.size = size;
        this.bufferSize = bufferSize;
        int numBuffers = calculateNumBuffers(size, bufferSize);
        try {
            this.buffers = initBuffers(numBuffers, size, bufferSize, supplier);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void destroy(ByteBuffer buffer);

    private ByteBuffer[] initBuffers(int numBuffers, long fileLength, int bufferSize, BiFunction<Long, Integer, ByteBuffer> supplier) {
        ByteBuffer[] buffers = new ByteBuffer[numBuffers];
        long total = 0;
        int size = bufferSize;
        for (int i = 0; i < numBuffers; i++) {
            if (i + 1 == numBuffers) {
                size = (int) (fileLength - total);
            }
            buffers[i] = supplier.apply(i * (long) size, size);
            total += size;
        }
        return buffers;
    }

    private int calculateNumBuffers(long fileLength, int bufferSize) {
        int numFullBuffers = (int) (fileLength / bufferSize);
        long diff = fileLength % bufferSize;
        return diff == 0 ? numFullBuffers : numFullBuffers + 1;
    }

    ByteBuffer getBuffer(long pos) {
        int idx = bufferIdx(pos);
        return buffers[idx];
    }

    private void destroyBuffers() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            closed.set(true);
            for (ByteBuffer buffer : buffers) {
                destroy(buffer);
            }
        } finally {
            lock.unlock();
        }
    }

    private int posOnBuffer(long pos) {
        return (int) (pos % Integer.MAX_VALUE);
    }

    int bufferIdx(long pos) {
        return (int) (pos / bufferSize);
    }

    int numBuffers() {
        return buffers.length;
    }

    private void checkClosed() {
        if (closed.get()) {
            throw new IllegalStateException("Closed storage");
        }
    }

    @Override
    public int write(ByteBuffer src) {
        Storage.ensureNonEmpty(src);

        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            checkClosed();
            if (!hasEnoughSpace(src.remaining())) {
                return EOF;
            }
            int dataSize = src.remaining();
            int written = 0;
            long currentPosition = writePosition();
            while (src.hasRemaining()) {
                ByteBuffer dst = getBuffer(currentPosition);

                int dstRemaining = dst.remaining();
                if (dstRemaining < dataSize) { //partial put
                    int srcLimit = src.limit();
                    int available = Math.min(dstRemaining, src.remaining());
                    src.limit(src.position() + available);
                    dst.put(src);
                    dst.flip();
                    src.limit(srcLimit);
                    written += dstRemaining;
                    currentPosition += dstRemaining;
                } else {
                    int srcRemaining = src.remaining();
                    dst.put(src);
                    written += srcRemaining;
                }
            }

            position.addAndGet(written);
            return written;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int read(long readPos, ByteBuffer dst) {

        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            checkClosed();
            if (!hasAvailableData(readPos)) {
                return EOF;
            }
            long writePosition = writePosition(); //use method instead direct position.get(): MMapCache delegates to disk

            int idx = bufferIdx(readPos);
            int bufferAddress = posOnBuffer(readPos);
            ByteBuffer buffer = getBuffer(idx);

            int srcCapacity = buffer.capacity();
            if (bufferAddress > srcCapacity) {
                throw new IllegalArgumentException("Invalid position " + readPos + ", buffer idx " + idx + ", buffer capacity " + srcCapacity);
            }

            ByteBuffer src = buffer.asReadOnlyBuffer();
            src.clear();
            src.position(bufferAddress);

            int dstRemaining = dst.remaining();
            int srcRemaining = src.remaining();
            if (dstRemaining > srcRemaining) {
                dst.put(src);
                if (idx + 1 >= buffers.length) { //no more buffers
                    return srcRemaining;
                }
                int read = read(readPos + srcRemaining, dst);
                return srcRemaining + (read >= 0 ? read : 0);
            }

            int available = (int) Math.min(readPos + dstRemaining, writePosition - readPos);
            int toBeCopied = Math.min(available, dstRemaining);
            src.limit(bufferAddress + toBeCopied);

            dst.put(src);
            return dstRemaining;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long length() {
        return size;
    }

    @Override
    public void writePosition(long position) {
        validateWriteAddress(position);
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            checkClosed();
            int idx = bufferIdx(position);
            ByteBuffer buffer = buffers[idx];
            int bufferAddress = posOnBuffer(position);
            buffer.position(bufferAddress);
            this.position.set(position);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long writePosition() {
        return position.get();
    }

    @Override
    public void delete() {
        destroyBuffers();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void close() {
        destroyBuffers();
    }

    @Override
    public void flush() {
        //do nothing
    }
}
