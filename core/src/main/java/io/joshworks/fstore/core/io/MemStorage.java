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
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final AtomicBoolean closed = new AtomicBoolean();
    protected final AtomicLong position = new AtomicLong();
    private final AtomicLong size = new AtomicLong();
    private final String name;

    protected final List<ByteBuffer> buffers = new ArrayList<>();

    MemStorage(String name, long size, BiFunction<Long, Integer, ByteBuffer> bufferSupplier) {
        this.name = name;
        this.bufferSupplier = bufferSupplier;
        this.buffers.addAll(initBuffers(size, bufferSupplier));
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
        int written = write(position(), src);
        position.addAndGet(written);
        return written;
    }

    @Override
    public int write(long position, ByteBuffer src) {
        Storage.ensureNonEmpty(src);

        ensureCapacity(position, src.remaining());
        Lock lock = readLock();
        try {
            checkClosed();
            return writeInternal(position, src);
        } finally {
            lock.unlock();
        }
    }

    protected void ensureCapacity(long position, long entrySize) {
        if (!hasEnoughSpace(entrySize, position) || position >= length()) {
            expand(entrySize, position);
        }
    }

    private int writeInternal(long position, ByteBuffer src) {
        int dataSize = src.remaining();
        int written = 0;
        while (src.hasRemaining()) {
            ByteBuffer dst = BufferUtil.getBuffer(buffers, position);

            int dstRemaining = dst.remaining();
            if (dstRemaining < dataSize) { //partial put
                int srcLimit = src.limit();
                int available = Math.min(dstRemaining, src.remaining());
                src.limit(src.position() + available);
                dst.put(src);
                dst.flip(); //this is important since there's no calls to clear when reading
                src.limit(srcLimit);
                written += dstRemaining;
                position += dstRemaining;
            } else {
                int srcRemaining = src.remaining();
                dst.put(src);
                written += srcRemaining;
            }
        }
        return written;
    }

    @Override
    public long write(ByteBuffer[] srcs) {
        long written = 0;
        long initialPosition = position();
        for (ByteBuffer src : srcs) {
            long currPos = initialPosition + written;
            ensureCapacity(currPos, src.remaining());
            written += writeInternal(currPos, src);
        }
        position.addAndGet(written);
        return written;
    }

    @Override
    public int read(long readPos, ByteBuffer dst) {
        if (!dst.hasRemaining()) {
            return 0;
        }
        Lock lock = readLock();
        try {
            checkClosed();
            long writePosition = position(); //use method instead direct position.get(): MMapCache delegates to disk

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
        return size.get();
    }

    @Override
    public void position(long position) {
        Lock lock = writeLockInterruptibly();
        try {
            checkClosed();
            int idx = BufferUtil.bufferIdx(buffers, position);
            if (idx >= 0 && idx < buffers.size()) {
                ByteBuffer buffer = buffers.get(idx);
                int bufferAddress = BufferUtil.posOnBuffer(buffers, position);
                buffer.position(bufferAddress);
            }
            this.position.set(position);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long position() {
        return position.get();
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
    public void truncate(long newSize) {
        if (newSize >= length()) {
            return;
        }
        Lock lock = writeLockInterruptibly();
        try {
            int idx = BufferUtil.bufferIdx(buffers, newSize);
            int bPos = BufferUtil.posOnBuffer(buffers, newSize);
            ByteBuffer bb = BufferUtil.getBuffer(buffers, newSize);
            bb.limit(bPos);
            bb.position(0);

            long unused = -1L;
            ByteBuffer newBuffer = bufferSupplier.apply(unused, bPos);
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
            position.accumulateAndGet(newSize, (curr, newPos) -> curr > newPos ? newPos : curr);
        } finally {
            lock.unlock();
        }
    }


    protected void expand(long entrySize, long position) {
        Lock lock = writeLockInterruptibly();
        try {
            long length = length();
            if (hasEnoughSpace(entrySize, position) && position < length) {
                return;
            }
            checkClosed();

            long byGrowRate = (long) (length * GROWTH_RATE);
            long minRequired = Math.max(entrySize, byGrowRate);
            //also adds the entry to be inserted, this guarantees that the entry will fit without needing further resizing
            long posLengthDiff = (position - length) + entrySize;
            //expand to current write position if write position is set or increase size by x%
            long requiredAdditionalLength = position > length ? posLengthDiff : minRequired;
            long expanded = 0;
            do {
                int normalized = (int) Math.min(MAX_BUFFER_SIZE, requiredAdditionalLength - expanded);
                expanded += normalized;
                long startPos = this.size.get();
                ByteBuffer newBuffer = bufferSupplier.apply(startPos, normalized);
                buffers.add(newBuffer);
            } while (expanded < requiredAdditionalLength);
            computeLength();

            int idx = BufferUtil.bufferIdx(buffers, position);
            int posOnBuffer = BufferUtil.posOnBuffer(buffers, position);
            buffers.get(idx).position(posOnBuffer);

        } finally {
            lock.unlock();
        }
    }

    //TODO not thread safe
    protected void computeLength() {
        this.size.set(buffers.stream().mapToLong(ByteBuffer::capacity).sum());
    }

    private void checkClosed() {
        if (closed.get()) {
            throw new StorageException("Closed storage");
        }
    }

    protected boolean hasEnoughSpace(long dataSize, long position) {
        long size = length();
        return position + dataSize <= size;
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
