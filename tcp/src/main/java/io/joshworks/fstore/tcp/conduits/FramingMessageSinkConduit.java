package io.joshworks.fstore.tcp.conduits;

import io.joshworks.fstore.core.io.buffers.StupidPool;
import org.xnio.conduits.AbstractStreamSinkConduit;
import org.xnio.conduits.StreamSinkConduit;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FramingMessageSinkConduit extends AbstractStreamSinkConduit<StreamSinkConduit> {

    private final StupidPool pool = new StupidPool(100, Integer.BYTES);

    public FramingMessageSinkConduit(StreamSinkConduit next) {
        super(next);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        ByteBuffer lenBuffer = pool.allocate();
        try {
            ByteBuffer b = lenBuffer.putInt(src.remaining()).flip();
            long written = super.write(new ByteBuffer[]{b, src}, 0, 2);
            if (written > Integer.MAX_VALUE) {
                throw new IllegalStateException("Written bytes was greater than " + Integer.MAX_VALUE);
            }
            return (int) written;
        } finally {
            pool.free(lenBuffer);
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offs, int len) throws IOException {
        ByteBuffer lenBuffer = pool.allocate();
        try {
            ByteBuffer[] withLen = new ByteBuffer[len];
            int j = 0;
            int totalLen = 0;
            withLen[j++] = lenBuffer;
            for (int i = offs; i < len; i++, j++) {
                ByteBuffer src = srcs[i];
                totalLen += src.remaining();
                withLen[j] = srcs[i];
            }
            withLen[0].putInt(totalLen).flip();
            return super.write(withLen, 0, withLen.length);

        } finally {
            pool.free(lenBuffer);
        }
    }

    @Override
    public int writeFinal(ByteBuffer src) throws IOException {
        ByteBuffer lenBuffer = pool.allocate();
        try {
            ByteBuffer b = lenBuffer.putInt(src.remaining()).flip();
            long written = super.writeFinal(new ByteBuffer[]{b, src}, 0, 2);
            if (written > Integer.MAX_VALUE) {
                throw new IllegalStateException("Written bytes was greater than " + Integer.MAX_VALUE);
            }
            return (int) written;
        } finally {
            pool.free(lenBuffer);
        }
    }

    @Override
    public long writeFinal(ByteBuffer[] srcs, int offset, int len) throws IOException {
        ByteBuffer lenBuffer = pool.allocate();
        try {
            ByteBuffer[] withLen = new ByteBuffer[len];
            int j = 0;
            int totalLen = 0;
            withLen[j++] = lenBuffer;
            for (int i = offset; i < len; i++, j++) {
                ByteBuffer src = srcs[i];
                totalLen += src.remaining();
                withLen[j] = srcs[i];
            }
            withLen[0].putInt(totalLen).flip();
            return super.write(withLen, 0, withLen.length);
        } finally {
            pool.free(lenBuffer);
        }
    }
}
