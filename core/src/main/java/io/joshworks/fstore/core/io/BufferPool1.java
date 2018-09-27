package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BufferPool1 implements BufferPool {

    private final int numBuffers;
    private final boolean direct;
    private int activeBuffers;
    private final BlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<>();
//    private final ThreadLocal<Cache> threadLocalCache = ThreadLocal.withInitial(this::getDefaultCache);
//    private final Cache defaultCache = new DefaultCache();

    public BufferPool1(int maxNumBuffers, boolean direct) {
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

    private static void zero(ByteBuffer buffer) {
        buffer.clear();
        while (buffer.remaining() >= 8) {
            buffer.putLong(0L);
        }
        while (buffer.hasRemaining()) {
            buffer.put((byte) 0);
        }
        buffer.clear();
    }
}
