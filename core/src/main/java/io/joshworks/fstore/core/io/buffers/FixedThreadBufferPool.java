package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;

/**
 * Single minimal 'fixed' sized buffer, cached per thread.
 * If the buffer is not big enough, a new allocated buffer is used
 * Throws IllegalState exception if the buffer is tried to be allocated without releasing it first
 */
public class FixedThreadBufferPool extends GrowingThreadBufferPool {

    private final int maxBufferSize;

    public FixedThreadBufferPool(int maxBufferSize) {
        this(maxBufferSize, false);
    }

    public FixedThreadBufferPool(int maxBufferSize, boolean direct) {
        super(direct);
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public ByteBuffer allocate(int size) {
        BufferHolder holder = cache.get();
        if (!holder.available.compareAndSet(true, false)) {
            throw new IllegalStateException("Buffer not released");
        }
        if (size > maxBufferSize) {
            return create(size);
        }
        holder.buffer.limit(size);
        return holder.buffer;
    }

    @Override
    public void free(ByteBuffer buffer) {
        if (cache.get().buffer.equals(buffer)) {
            buffer.clear();
            cache.get().available.set(true);
        }
    }


}
