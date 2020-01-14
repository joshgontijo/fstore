package io.joshworks.fstore.core.io.buffers;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleBufferPool implements BufferPool {

    private final ThreadLocal<Queue<BufferRef>> threadAllocations = ThreadLocal.withInitial(ArrayDeque::new);
    private final Queue<BufferRef> pool = new ConcurrentLinkedQueue<>();
    private final int bufferSize;
    private final boolean direct;

    public SimpleBufferPool(int bufferSize, boolean direct) {
        this.bufferSize = bufferSize;
        this.direct = direct;
    }

    //allocate current buffer with its total capacity
    @Override
    public ByteBuffer allocate() {
        BufferRef bufferRef = allocateRef();
        return bufferRef.buffer;
    }

    public BufferRef allocateRef() {
        BufferRef bufferRef = pool.poll();
        if (bufferRef == null) {
            bufferRef = new BufferRef();
        }
        bufferRef.available.set(false);
        threadAllocations.get().add(bufferRef);
        return bufferRef;
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
        BufferRef ref;
        Queue<BufferRef> allocations = this.threadAllocations.get();
        while ((ref = allocations.poll()) != null) {
            ref.free();
        }
    }

    public final class BufferRef implements Closeable {
        public final ByteBuffer buffer = Buffers.allocate(bufferSize, direct);
        final AtomicBoolean available = new AtomicBoolean(false);

        private BufferRef() {
        }

        public void free() {
            if (available.compareAndSet(false, true)) {
                buffer.clear();
                pool.add(this);
                available.set(true);
            }
        }

        @Override
        public void close() {
            free();
        }
    }

}
