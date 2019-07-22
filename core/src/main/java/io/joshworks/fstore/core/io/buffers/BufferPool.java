package io.joshworks.fstore.core.io.buffers;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.joshworks.fstore.core.io.MemStorage.MAX_BUFFER_SIZE;

/**
 * Single dynamic sized buffer, cached per thread.
 * Throws IllegalState exception if the buffer is tried to be allocated without releasing it first
 * A single thread may only allocate a single buffer at the time,
 * allocating more than once without freeing the previous buffer will corrupt the buffer contents
 */
public class BufferPool implements Closeable {

    private final ThreadLocal<BufferHolder> cache;
    private final int maxSize;
    private final boolean direct;

    public BufferPool(int size) {
        this(size, false);
    }

    public BufferPool(int maxSize, boolean direct) {
        if (maxSize >= MAX_BUFFER_SIZE) {
            throw new IllegalArgumentException("Buffer too large: Max allowed size is: " + MAX_BUFFER_SIZE);
        }
        this.maxSize = maxSize;
        this.direct = direct;
        this.cache = ThreadLocal.withInitial(() -> new BufferHolder(create(maxSize)));
    }

    private ByteBuffer create(int size) {
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    //allocate current buffer with its total capacity
    public ByteBuffer allocate() {
        return allocate(maxSize);
    }

    public boolean direct() {
        return direct;
    }

    public int capacity() {
        return maxSize;
    }

//    public void resize(int size) {
//        BufferHolder holder = cache.get();
//        if (!holder.available.compareAndSet(true, false)) {
//            throw new IllegalStateException("Buffer not released");
//        }
//        holder.buffer = create(size);
//        holder.available.set(true);
//    }

    public ByteBuffer allocate(int size) {
        BufferHolder holder = cache.get();
        if (size > maxSize) {
            throw new IllegalArgumentException("Cannot allocate buffer bigger than " + holder.buffer.capacity());
        }
        if (!holder.available.compareAndSet(true, false)) {
            throw new IllegalStateException("Buffer not released");
        }
        if (size > 0 && holder.buffer.limit() < size) {
            holder.buffer = create(size);
        }
        if (size > 0) {
            holder.buffer.limit(size);
        }
        return holder.buffer;
    }

    public void free() {
        cache.get().free();
    }

    @Override
    public void close() {
        free();
    }

    static final class BufferHolder {
        ByteBuffer buffer;
        final AtomicBoolean available = new AtomicBoolean(true);

        private BufferHolder(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        private void free() {
            buffer.clear();
            available.set(true);
        }
    }

}
