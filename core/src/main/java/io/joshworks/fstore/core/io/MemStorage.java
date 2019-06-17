package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.BufferUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

public abstract class MemStorage implements Storage {

    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;
    private static final double GROWTH_RATE = 0.5; //50%

    //    protected final int bufferSize;
    private final BiFunction<Long, Integer, ByteBuffer> bufferSupplier;
    protected final AtomicLong writePosition = new AtomicLong();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final String name;
    private final AtomicLong length = new AtomicLong();

    protected final List<ByteBuffer> buffers = new ArrayList<>();

    MemStorage(String name, long length, BiFunction<Long, Integer, ByteBuffer> bufferSupplier) {
        this.name = name;
        this.bufferSupplier = bufferSupplier;
        this.buffers.addAll(initBuffers(length, bufferSupplier));
        computeLength();
    }

    protected abstract void destroy(ByteBuffer buffer);

    protected List<ByteBuffer> initBuffers(long fileLength, BiFunction<Long, Integer, ByteBuffer> supplier) {
        List<ByteBuffer> buffers = new ArrayList<>();
        long total = 0;
        while (total < fileLength) {
            int bufferSize = (int) Math.min(fileLength - total, MAX_BUFFER_SIZE);
            ByteBuffer buffer = supplier.apply(total, bufferSize);
            buffers.add(buffer);
            total += bufferSize;
        }
        return buffers;
    }

    @Override
    public int write(ByteBuffer src) {
        Storage.ensureNonEmpty(src);

        if (!hasEnoughSpace(src.remaining())) {
            expand(src.remaining());
        }

        Lock lock = readLock();
        try {
            checkClosed();
            int dataSize = src.remaining();
            int written = 0;
            long currentPosition = writePosition();
            while (src.hasRemaining()) {
                ByteBuffer dst = BufferUtil.getBuffer(buffers, currentPosition);

                int dstRemaining = dst.remaining();
                if (dstRemaining < dataSize) { //partial put
                    int srcLimit = src.limit();
                    int available = Math.min(dstRemaining, src.remaining());
                    src.limit(src.position() + available);
                    dst.put(src);
                    dst.flip(); //this is important since there's no calls to clear when reading
                    src.limit(srcLimit);
                    written += dstRemaining;
                    currentPosition += dstRemaining;
                } else {
                    int srcRemaining = src.remaining();
                    dst.put(src);
                    written += srcRemaining;
                }
            }

            writePosition.addAndGet(written);
            return written;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int read(long readPos, ByteBuffer dst) {
        if (!dst.hasRemaining()) {
            return 0;
        }
        Lock lock = readLock();
        try {
            checkClosed();
            long writePosition = writePosition(); //use method instead direct position.get(): MMapCache delegates to disk

            int idx = BufferUtil.bufferIdx(buffers, readPos);
            int bufferAddress = BufferUtil.posOnBuffer(buffers, readPos);

            int dstPos = dst.position();
            while (dst.hasRemaining() && idx < buffers.size() && readPos < writePosition) {
                ByteBuffer src = buffers.get(idx).asReadOnlyBuffer();
                long maxPosition = Math.min(readPos + dst.remaining(), writePosition);
                src.clear();
                src.position(bufferAddress);

                int toBeCopied = (int) Math.min(maxPosition - readPos, src.remaining());

                src.limit(bufferAddress + toBeCopied);
                dst.put(src);

                bufferAddress = 0; //reset buffer address, so the next start from pos zero
                readPos += toBeCopied;
                idx++;
            }
            int read = dst.position() - dstPos;
            return read == 0 && readPos >= writePosition ? EOF : read;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long length() {
        return length.get();
    }

    @Override
    public void writePosition(long position) {
        validateWriteAddress(position);
        Lock lock = writeLockInterruptibly();
        try {
            checkClosed();
            int idx = BufferUtil.bufferIdx(buffers, position);
            if (idx < buffers.size()) {
                ByteBuffer buffer = buffers.get(idx);
                int bufferAddress = BufferUtil.posOnBuffer(buffers, position);
                buffer.position(bufferAddress);
            }
            this.writePosition.set(position);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long writePosition() {
        return writePosition.get();
    }

    @Override
    public void delete() {
        close();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Lock lock = writeLockInterruptibly();
            try {
                for (ByteBuffer buffer : buffers) {
                    destroy(buffer);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void flush() {
        //do nothing
    }

    @Override
    public void truncate() {
        Lock lock = writeLockInterruptibly();
        try {
            long pos = writePosition.get();
            int idx = BufferUtil.bufferIdx(buffers, pos);
            int bPos = BufferUtil.posOnBuffer(buffers, pos);
            ByteBuffer bb = BufferUtil.getBuffer(buffers, pos);
            bb.limit(bPos);
            bb.position(0);

            long unused = -1L;
            ByteBuffer newBuffer = bufferSupplier.apply(unused, bPos + 1);
            newBuffer.put(bb);
            buffers.set(idx, newBuffer);

            if (bPos == 0) {
                buffers.remove(idx);
            }
            for (int i = idx + 1; i < buffers.size(); i++) {
                ByteBuffer removed = buffers.remove(idx);
                destroy(removed);
            }
            computeLength();
        } finally {
            lock.unlock();
        }
    }


    protected void expand(int entrySize) {
        Lock lock = writeLockInterruptibly();
        try {
            checkClosed();
            long additionalSpace = (long) (length() * GROWTH_RATE);
            int normalized = (int) Math.min(MAX_BUFFER_SIZE, additionalSpace);
            int bufferSize = Math.max(entrySize, normalized);
            long startPos = length.get();
            ByteBuffer newBuffer = bufferSupplier.apply(startPos, bufferSize);
            buffers.add(newBuffer);
            computeLength();
        } finally {
            lock.unlock();
        }
    }

    //TODO not thread safe
    protected void computeLength() {
        this.length.set(buffers.stream().mapToLong(ByteBuffer::capacity).sum());
    }

    private void checkClosed() {
        if (closed.get()) {
            throw new StorageException("Closed storage");
        }
    }

    protected Lock readLock() {
        Lock lock = rwLock.readLock();
        lock.lock();
        return lock;
    }

    protected Lock writeLockInterruptibly() {
        try {
            Lock writeLock = rwLock.writeLock();
            writeLock.lockInterruptibly();
            return writeLock;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException(e);
        }
    }

}
