package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;

public interface BufferPool extends Pool<ByteBuffer> {

    boolean direct();

    int bufferSize();

    void free();

    @Override
    default void close() {
        free();
    }

}
