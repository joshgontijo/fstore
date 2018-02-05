package io.joshworks.fstore.serializer;

import io.joshworks.fstore.api.Serializer;

import java.nio.ByteBuffer;

public class ByteSerializer implements Serializer<Byte> {

    @Override
    public ByteBuffer toBytes(Byte data) {
        return ByteBuffer.allocate(Byte.BYTES).put(data).flip();
    }

    @Override
    public Byte fromBytes(ByteBuffer data) {
        return data.get();
    }
}
