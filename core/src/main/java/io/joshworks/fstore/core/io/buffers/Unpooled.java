package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;

/**
 * Always allocate a new buffer, not a pool despite the name
 */
class Unpooled implements BufferPool {

    private final int bufferSize;
    private final boolean direct;

    Unpooled(int bufferSize, boolean direct) {
        this.direct = direct;
        this.bufferSize = bufferSize;
    }


    @Override
    public ByteBuffer allocate() {
        return Buffers.allocate(bufferSize, direct);
    }

    @Override
    public void free(ByteBuffer element) {
        //do nothing
    }


}
