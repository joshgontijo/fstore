package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class ByteArraySerializer extends SizePrefixedArraySerializer<byte[]> {

    @Override
    public ByteBuffer toBytes(byte[] data) {
        ByteBuffer bb = allocate(data.length);
        bb.put(data);
        return bb.flip();
    }

    @Override
    public void writeTo(byte[] data, ByteBuffer dst) {
        dst.putInt(data.length);
        dst.put(data);
    }

    @Override
    public byte[] fromBytes(ByteBuffer data) {
        byte[] b = new byte[data.remaining()];
        data.get(b);
        return b;
    }

    @Override
    int byteSize() {
        return Byte.BYTES;
    }
}
