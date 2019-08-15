package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.BufferPool;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Copies the available data of a given buffer into a new one.
 * The copy will be a direct buffer if the original is also a direct buffer.
 */
public class ByteBufferTransfer implements Serializer<ByteBuffer> {

    private final BufferPool pool;
    private final ByteBuffer buffer;

    public ByteBufferTransfer(BufferPool pool) {
        this.pool = requireNonNull(pool);
        this.buffer = null;
    }

    public ByteBufferTransfer(ByteBuffer buffer) {
        this.buffer = requireNonNull(buffer);
        this.pool = null;
    }

    @Override
    public void writeTo(ByteBuffer data, ByteBuffer dst) {
        dst.put(data);
    }

    @Override
    public ByteBuffer fromBytes(ByteBuffer buffer) {
        return copy(buffer);
    }

    private ByteBuffer copy(ByteBuffer original) {
        int dataSize = original.remaining();
        var dst = pool != null ? pool.allocate() : buffer;
        if (dst.limit() < dataSize) {
            throw new IllegalArgumentException("Destination buffer is smaller than source data size");
        }
        dst.put(original);
        dst.flip();
        return dst;
    }
}
