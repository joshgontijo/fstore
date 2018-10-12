package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AdaptiveBufferPool implements BufferPool {

    private final int numBuffers;
    private final boolean direct;
    private int activeBuffers;
    private final BlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<>();

    public AdaptiveBufferPool(int maxNumBuffers, boolean direct) {
        this.numBuffers = maxNumBuffers;
        this.direct = direct;
    }

    @Override
    public ByteBuffer allocate(int size) {
        ByteBuffer buffer = buffers.poll();
        //discard smaller buffers, keeps maxNumBuffers of the biggest
        if (buffer != null && buffer.capacity() >= size) {
            buffer.limit(size);
            return buffer;
        }

        if (activeBuffers >= numBuffers) {
            return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
        }

        synchronized (this) {
            buffer = direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
            activeBuffers++;
            return buffer;
        }

    }

    @Override
    public void free(ByteBuffer buffer) {
        buffer.clear();
        buffers.add(buffer);
    }
}
