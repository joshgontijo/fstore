package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;

/**
 * Always allocate a new buffer, not a pool despite the name
 */
public class DiscardBufferPool implements BufferPool {

    private final int bufferSize;
    private final boolean direct;

    public DiscardBufferPool(int bufferSize, boolean direct) {
        this.direct = direct;
        this.bufferSize = bufferSize;
    }


    @Override
    public int bufferSize() {
        return 0;
    }

    @Override
    public void free() {

    }

    @Override
    public ByteBuffer allocate() {
        return Buffers.allocate(bufferSize, direct);
    }

    @Override
    public boolean direct() {
        return direct;
    }

}
