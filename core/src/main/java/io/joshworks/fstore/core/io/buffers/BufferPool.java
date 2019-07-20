package io.joshworks.fstore.core.io.buffers;

import io.joshworks.fstore.core.util.Memory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single dynamic sized buffer, cached per thread.
 * Throws IllegalState exception if the buffer is tried to be allocated without releasing it first
 * A single thread may only allocate a single buffer at the time,
 * allocating more than once without freeing the previous buffer will corrupt the buffer contents
 */
public class BufferPool {

    private final ThreadLocal<BufferHolder> cache;
    private final boolean direct;

    public BufferPool() {
        this(false);
    }

    public BufferPool(boolean direct) {
        this.direct = direct;
        this.cache = ThreadLocal.withInitial(() -> new BufferHolder(create(Memory.PAGE_SIZE)));
    }

    public BufferPool(boolean direct, int initialSize) {
        this.direct = direct;
        this.cache = ThreadLocal.withInitial(() -> new BufferHolder(create(initialSize)));
    }

    protected ByteBuffer create(int size) {
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    //allocate current buffer with its total capacity
    public ByteBuffer allocate() {
        return allocate(-1);
    }

    public int bufferCapacity() {
        return cache.get().buffer.capacity();
    }

    public void resize(int size) {
        BufferHolder holder = cache.get();
        if (!holder.available.compareAndSet(true, false)) {
            throw new IllegalStateException("Buffer not released");
        }
        holder.buffer = create(size);
        holder.available.set(true);
    }

    public ByteBuffer allocate(int size) {
        BufferHolder holder = cache.get();
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

    public void free(ByteBuffer buffer) {
        buffer.clear();
        cache.get().available.set(true);
    }

    static final class BufferHolder {
        ByteBuffer buffer;
        final AtomicBoolean available = new AtomicBoolean(true);

        private BufferHolder(ByteBuffer buffer) {
            this.buffer = buffer;
        }
    }

}
