package io.joshworks.fstore.core.io.buffers;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface BufferPool extends Closeable {

    ByteBuffer allocate();

    boolean direct();

    int bufferSize();

    void free();

    @Override
    default void close() {
        free();
    }

}
