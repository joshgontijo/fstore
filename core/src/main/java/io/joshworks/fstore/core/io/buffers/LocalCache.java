package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single dynamic sized buffer, cached per thread.
 * Attempts to allocate buffer from thread local cache first, if not available then resorts to the master queue
 */
class LocalCache implements BufferPool {

    private final ThreadLocal<Holder> cache = ThreadLocal.withInitial(Holder::new);
    private final int bufferSize;
    private final boolean direct;

    public LocalCache(int bufferSize, boolean direct) {
        this.bufferSize = bufferSize;
        this.direct = direct;
    }

    @Override
    public ByteBuffer allocate() {
        Holder holder = cache.get();
        if (!holder.available.compareAndSet(true, false)) {
            throw new IllegalStateException("Buffer not released yet");
        }
        return holder.buffer;
    }

    @Override
    public void free(ByteBuffer buffer) {
        Holder cacheHolder = cache.get();
        if (buffer == cacheHolder.buffer) {
            cacheHolder.free();
        } else {
            throw new IllegalArgumentException("Buffer being released is not the same as the allocated");
        }
    }

    private final class Holder {
        final ByteBuffer buffer = Buffers.allocate(bufferSize, direct);
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
