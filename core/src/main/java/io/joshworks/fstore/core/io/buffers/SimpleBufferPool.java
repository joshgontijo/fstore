package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleBufferPool implements BufferPool {

    private final ThreadLocal<Queue<BufferRef>> cache = ThreadLocal.withInitial(ArrayDeque::new);
    private final Queue<BufferRef> pool = new ConcurrentLinkedQueue<>();
    private final int bufferSize;
    private final boolean direct;

    public SimpleBufferPool(int bufferSize) {
        this(bufferSize, false);
    }

    public SimpleBufferPool(int bufferSize, boolean direct) {
        this.bufferSize = bufferSize;
        this.direct = direct;
    }

    //allocate current buffer with its total capacity
    @Override
    public ByteBuffer allocate() {
        BufferRef bufferRef = pool.poll();
        if (bufferRef == null) {
            bufferRef = new BufferRef();
            cache.get().add(bufferRef);
        }
        if (!bufferRef.available.compareAndSet(true, false)) {
            throw new IllegalStateException("Buffer not released");
        }
        return bufferRef.buffer;
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
    public void close() {
        BufferRef ref;
        Queue<BufferRef> allocations = cache.get();
        while ((ref = allocations.poll()) != null) {
            ref.free();
        }
    }

    private final class BufferRef {
        final ByteBuffer buffer = Buffers.allocate(bufferSize, direct);
        final AtomicBoolean available = new AtomicBoolean(true);

        void free() {
            buffer.clear();
            available.set(true);
            pool.add(this);
        }
    }

}
