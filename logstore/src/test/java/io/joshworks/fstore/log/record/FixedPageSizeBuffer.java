package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.util.Memory;

import java.nio.ByteBuffer;

public class FixedPageSizeBuffer implements BufferPool {
    @Override
    public ByteBuffer allocate(int bytes) {
        return ByteBuffer.allocate(bytes);
    }

    @Override
    public void free(ByteBuffer buffer) {

    }
}
