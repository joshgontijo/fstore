package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single dynamic sized buffer, cached per thread.
 * Attempts to allocate buffer from thread local cache first, if not available then resorts to the master queue
 */
class CachedBufferPool extends BasicBufferPool {

    private final ThreadLocal<Holder> cache = ThreadLocal.withInitial(Holder::new);

    public CachedBufferPool(int maxItems, int bufferSize, boolean direct) {
        super(maxItems, bufferSize, direct);
    }

    @Override
    public ByteBuffer allocate() {
        Holder holder = cache.get();
        if (!holder.available.compareAndSet(true, false)) {
            return super.allocate();
        }
        return holder.buffer;
    }

    @Override
    public void free(ByteBuffer buffer) {
        Holder cacheHolder = this.cache.get();
        if (buffer == cacheHolder.buffer) {
            cacheHolder.free();
        } else {
            super.free(buffer);
        }
    }

    private final class Holder {
        final ByteBuffer buffer = CachedBufferPool.super.allocate();
        final AtomicBoolean available = new AtomicBoolean(true);

        public Holder() {
        }

        void free() {
            if (available.compareAndSet(false, true)) {
                buffer.clear();
                available.set(true);
            }
        }
    }

}
