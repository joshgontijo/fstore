package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;

public interface BufferPool {

    ByteBuffer allocate(int bytes);

    void free(ByteBuffer buffer);

}
