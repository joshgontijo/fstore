package io.joshworks.fstore.core.io.buffers;

import io.joshworks.fstore.core.util.Memory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single dynamic sized buffer, cached per thread.
 * Throws IllegalState exception if the buffer is tried to be allocated without releasing it first
 */
public class LocalGrowingBufferPool implements BufferPool {

    protected final ThreadLocal<BufferHolder> cache;
    protected final boolean direct;

    public LocalGrowingBufferPool() {
        this(false);
    }

    public LocalGrowingBufferPool(boolean direct) {
        this.direct = direct;
        this.cache = ThreadLocal.withInitial(() -> new BufferHolder(create(Memory.PAGE_SIZE)));
    }

    protected ByteBuffer create(int size) {
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer allocate(int size) {
        BufferHolder holder = cache.get();
        if (!holder.available.compareAndSet(true, false)) {
            throw new IllegalStateException("Buffer not released");
        }
        if (holder.buffer.limit() < size) {
            holder.buffer = create(size);
        }
        holder.buffer.limit(size);
        return holder.buffer;
    }

    @Override
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
