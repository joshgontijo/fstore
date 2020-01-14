package io.joshworks.fstore.core.io.buffers;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class StupidPool2 {

    private final BlockingQueue<Ref> pool;
    private final int bufferSize;
    private final boolean direct;

    public StupidPool2(int maxItems, int bufferSize) {
        this(maxItems, bufferSize, false);
    }

    public StupidPool2(int maxItems, int bufferSize, boolean direct) {
        this.pool = new ArrayBlockingQueue<>(maxItems);
        this.bufferSize = bufferSize;
        this.direct = direct;
    }

    public Ref allocate() {
        Ref instance = pool.poll();
        return instance == null ? new Ref(this, bufferSize, direct) : instance;
    }

    public static class Ref implements Closeable {
        private boolean allocated;
        private final StupidPool2 poolRef;
        public final ByteBuffer buffer;

        private Ref(StupidPool2 poolRef, int bufferSize, boolean direct) {
            this.poolRef = poolRef;
            this.buffer = Buffers.allocate(bufferSize, direct);
        }

        @Override
        public void close() {
            if (!allocated) {
                return;
            }
            allocated = false;
            buffer.clear();
            poolRef.pool.offer(this);
        }
    }

}
