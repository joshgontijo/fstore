package io.joshworks.eventry.server.tcp_xnio.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import org.xnio.Pooled;

import java.nio.ByteBuffer;

public class BufferPoolAdaptor extends BufferPool implements Pooled<ByteBuffer> {

    public BufferPoolAdaptor(int maxSize, boolean direct) {
        super(maxSize, direct);
    }

    @Override
    public ByteBuffer getResource() throws IllegalStateException {
        return super.allocate();
    }

    @Override
    public void discard() {
        //do nothing
    }
}
