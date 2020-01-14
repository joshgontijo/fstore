package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class StupidPool {

    private final BlockingQueue<ByteBuffer> pool;
    private final int bufferSize;

    public StupidPool(int maxItems, int bufferSize) {
        this.pool = new ArrayBlockingQueue<>(maxItems);
        this.bufferSize = bufferSize;
    }

    public ByteBuffer allocate() {
        ByteBuffer instance = pool.poll();
        return instance == null ? Buffers.allocate(bufferSize, false) : instance;
    }

    public void free(ByteBuffer ref) {
        if (ref == null) {
            return;
        }
        pool.offer(ref.clear());
    }

}
