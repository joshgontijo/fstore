package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.buffers.BufferPool;

import java.nio.ByteBuffer;

public class FixedPageSizeBuffer implements BufferPool {
    @Override
    public ByteBuffer allocate(int bytes) {
        return ByteBuffer.allocate(bytes);
    }

    @Override
    public void free(ByteBuffer buffer) {
        //do nothing
    }
}
