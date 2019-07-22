package io.joshworks.fstore.serializer.arrays;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class ByteArraySerializer implements Serializer<byte[]> {

    @Override
    public void writeTo(byte[] data, ByteBuffer dst) {
        dst.putInt(data.length);
        dst.put(data);
    }

    @Override
    public byte[] fromBytes(ByteBuffer data) {
        int size = data.getInt();
        byte[] b = new byte[size];
        data.get(b);
        return b;
    }
}
