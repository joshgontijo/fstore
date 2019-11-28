package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

public class ByteBufferCopy implements Serializer<ByteBuffer> {
    @Override
    public void writeTo(ByteBuffer data, ByteBuffer dst) {
        dst.put(data);
    }

    @Override
    public ByteBuffer fromBytes(ByteBuffer buffer) {
        ByteBuffer copy = Buffers.allocate(buffer.remaining(), buffer.isDirect());
        copy.put(buffer);
        copy.flip();
        return copy;
    }
}
