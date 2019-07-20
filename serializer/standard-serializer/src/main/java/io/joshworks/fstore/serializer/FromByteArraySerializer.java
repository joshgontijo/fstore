package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class FromByteArraySerializer implements Serializer<byte[]> {

    @Override
    public ByteBuffer toBytes(byte[] data) {
        return ByteBuffer.wrap(data);
    }

    @Override
    public void writeTo(byte[] data, ByteBuffer dst) {
        dst.put(data);
    }

    @Override
    public byte[] fromBytes(ByteBuffer data) {
        if (data.isDirect()) {
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            return bytes;
        }
        return data.array();
    }
}
