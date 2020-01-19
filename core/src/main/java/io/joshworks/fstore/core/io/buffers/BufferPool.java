package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface BufferPool {

    ByteBuffer allocate();

    void free(ByteBuffer buffer);

    default void withBuffer(Consumer<ByteBuffer> func) {
        ByteBuffer buffer = allocate();
        try {
            func.accept(buffer);
        } finally {
            free(buffer);
        }
    }

    default <R> R withBuffer(Function<ByteBuffer, R> func) {
        ByteBuffer buffer = allocate();
        try {
            return func.apply(buffer);
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

    static BufferPool unpooled(int bufferSize, boolean direct) {
        return new Unpooled(bufferSize, direct);
    }

}
