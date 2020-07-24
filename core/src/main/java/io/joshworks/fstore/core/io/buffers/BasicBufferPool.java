package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Queue based cache, cache up to maxItems, thread safe
 * Does not use thread local cache, not doing anything smart, just reusing buffers as they get freed.
 */
class BasicBufferPool implements BufferPool {

    private final Queue<ByteBuffer> pool;
    private final int bufferSize;
    private final boolean direct;

    BasicBufferPool(int maxItems, int bufferSize, boolean direct) {
        this.pool = new ArrayBlockingQueue<>(maxItems);
        this.bufferSize = bufferSize;
        this.direct = direct;
    }

    public ByteBuffer allocate() {
        ByteBuffer instance = pool.poll();
        return instance == null ? Buffers.allocate(bufferSize, direct) : instance;
    }

    public void free(ByteBuffer element) {
        if (element == null) {
            return;
        }
        pool.offer(element.clear());
    }

}
