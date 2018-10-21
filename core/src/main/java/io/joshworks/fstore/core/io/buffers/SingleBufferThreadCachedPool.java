package io.joshworks.fstore.core.io.buffers;

import io.joshworks.fstore.core.util.Memory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single dynamic sized buffer, cached per thread.
 * Throws IllegalState exception if the buffer is tried to be allocated without releasing it first
 */
public class SingleBufferThreadCachedPool implements BufferPool {

    private final ThreadLocal<BufferHolder> cache;
    private final boolean direct;

    public SingleBufferThreadCachedPool() {
        this(false);
    }

    public SingleBufferThreadCachedPool(boolean direct) {
        this.direct = direct;
        this.cache = ThreadLocal.withInitial(() -> new BufferHolder(create(Memory.PAGE_SIZE)));
    }

    private ByteBuffer create(int size) {
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer allocate(int size) {
        BufferHolder holder = cache.get();
        if(!holder.available.compareAndSet(true, false)) {
            throw new IllegalStateException("Buffer not released");
        }
        if(holder.buffer.limit() < size) {
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

    private static final class BufferHolder {
        private ByteBuffer buffer;
        private final AtomicBoolean available = new AtomicBoolean(true);

        private BufferHolder(ByteBuffer buffer) {
            this.buffer = buffer;
        }
    }

}
