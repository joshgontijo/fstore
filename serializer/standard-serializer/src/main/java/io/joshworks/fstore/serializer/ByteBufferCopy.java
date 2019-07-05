package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

/**
 * Copies the available data of a given buffer into a new one.
 * The copy will be a direct buffer if the original is also a direct buffer.
 */
public class ByteBufferCopy implements Serializer<ByteBuffer> {

    @Override
    public ByteBuffer toBytes(ByteBuffer data) {
        return copy(data);
    }

    @Override
    public void writeTo(ByteBuffer data, ByteBuffer dest) {
        dest.put(data);
    }

    @Override
    public ByteBuffer fromBytes(ByteBuffer buffer) {
        return copy(buffer);
    }

    private ByteBuffer copy(ByteBuffer original) {
        int remaining = original.remaining();
        ByteBuffer clone = original.isDirect() ? ByteBuffer.allocateDirect(remaining) : ByteBuffer.allocate(remaining);
        clone.put(original);
        clone.flip();
        return clone;
    }
}
