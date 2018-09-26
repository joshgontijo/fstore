package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BufferPool1 implements BufferPool {

    private final int bufferSize;
    private final int numBuffers;
    private final boolean direct;
    private int activeBuffers;
    private final BlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<>();
//    private final ThreadLocal<Cache> threadLocalCache = ThreadLocal.withInitial(this::getDefaultCache);
//    private final Cache defaultCache = new DefaultCache();

    public BufferPool1(int bufferSize, int numBuffers, boolean direct) {
        this.bufferSize = bufferSize;
        this.numBuffers = numBuffers;
        this.direct = direct;
    }

    @Override
    public ByteBuffer allocate(int bytes) {
        ByteBuffer buffer = buffers.poll();
        if (buffer == null) {
            if (activeBuffers < numBuffers) {
                synchronized (this) {
                    if (activeBuffers < numBuffers) {
                        buffer = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
                        activeBuffers++;
                        return buffer;
                    }
                }
            }
            try {
                buffer = buffers.poll(10, TimeUnit.SECONDS);
                if (buffer == null) {
                    throw new RuntimeException("No buffer was available after 10s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return buffer;
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
