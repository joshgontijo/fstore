package io.joshworks.fstore.core.io.buffers;

import io.joshworks.fstore.core.metrics.MetricRegistry;
import io.joshworks.fstore.core.metrics.Metrics;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single dynamic sized buffer, cached per thread.
 * Throws IllegalState exception is thrown if an attempt to allocate a buffer without releasing it first occurs
 * A single thread may only allocate a single buffer at the time,
 * allocating more than once without freeing the previous buffer will corrupt the buffer contents
 */
public class ThreadLocalBufferPool implements BufferPool {

    private final int bufferSize;
    private final boolean direct;


    private final ThreadLocal<Holder> cache = ThreadLocal.withInitial(Holder::new);

    private final Metrics metrics = new Metrics();

    public ThreadLocalBufferPool(String name, int bufferSize, boolean direct) {
        this.bufferSize = bufferSize;
        this.direct = direct;

        this.metrics.set("bufferSize", bufferSize);
        MetricRegistry.register(Map.of("type", "bufferPools", "impl", "ThreadLocalBufferPool", "name", name), () -> metrics);
    }

    //allocate current buffer with its total capacity
    @Override
    public ByteBuffer allocate() {
        Holder holder = cache.get();
        if (!holder.available.compareAndSet(true, false)) {
            throw new IllegalStateException("Buffer not released");
        }
        metrics.update("allocated", 1);
        return holder.buffer;
    }

    @Override
    public boolean direct() {
        return direct;
    }

    @Override
    public int bufferSize() {
        return bufferSize;
    }

    @Override
    public void free() {
        cache.get().free();
    }

    private final class Holder {
        final ByteBuffer buffer = Buffers.allocate(bufferSize, direct);
        final AtomicBoolean available = new AtomicBoolean(true);

        public Holder() {
            metrics.update("buffers");
        }

        void free() {
            if (available.compareAndSet(false, true)) {
                buffer.clear();
                available.set(true);
                metrics.update("allocated", -1);
            }
        }
    }

}
