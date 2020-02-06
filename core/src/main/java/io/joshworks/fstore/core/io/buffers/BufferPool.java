package io.joshworks.fstore.core.io.buffers;

import io.joshworks.fstore.core.util.Pool;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface BufferPool extends Pool<ByteBuffer> {

    default void withBuffer(Consumer<ByteBuffer> func) {
        ByteBuffer buffer = allocate();
        try {
            func.accept(buffer);
        } finally {
            free(buffer);
        }
    }

    static BufferPool defaultPool(int poolSize, int bufferSize, boolean direct) {
        return new BasicBufferPool(poolSize, bufferSize, direct);
    }

    static BufferPool localCachePool(int poolSize, int bufferSize, boolean direct) {
        return new CachedBufferPool(poolSize, bufferSize, direct);
    }

    static BufferPool localCache(int bufferSize, boolean direct) {
        return new LocalCache(bufferSize, direct);
    }

    static BufferPool unpooled(int bufferSize, boolean direct) {
        return new Unpooled(bufferSize, direct);
    }

}
