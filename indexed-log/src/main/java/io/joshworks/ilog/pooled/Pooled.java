package io.joshworks.ilog.pooled;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.Closeable;
import java.nio.ByteBuffer;

public abstract class Pooled implements Closeable {

    private final ObjectPool<? extends Pooled> pool;
    protected final ByteBuffer data;

    Pooled(ObjectPool<? extends Pooled> pool, int size, boolean direct) {
        this.pool = pool;
        this.data = Buffers.allocate(size, direct);
    }

    @Override
    public void close()  {
        pool.release(this);
    }
}
