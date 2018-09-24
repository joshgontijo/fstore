package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class BufferPool {

    private final int bufferSize;
    private final int maxBuffers;
    private final boolean direct;
    private int counter;
    private final BlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<>();
//    private final ThreadLocal<Cache> threadLocalCache = ThreadLocal.withInitial(this::getDefaultCache);
//    private final Cache defaultCache = new DefaultCache();

    public BufferPool(int bufferSize, int maxBuffers, boolean direct) {
        this.bufferSize = bufferSize;
        this.maxBuffers = maxBuffers;
        this.direct = direct;
    }

    public ByteBuffer allocate() {
        ByteBuffer buffer = buffers.poll();
        if (buffer == null) {
            if (counter < maxBuffers) {
                synchronized (this) {
                    if (counter < maxBuffers) {
                        buffer = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
                        counter++;
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

    public void free(ByteBuffer buffer) {
        zero(buffer);
        buffers.add(buffer);
    }

    public void runWith(Consumer<ByteBuffer> consumer) {
        ByteBuffer buffer = allocate();
        try {
            consumer.accept(buffer);
        } finally {
            free(buffer);
        }
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
