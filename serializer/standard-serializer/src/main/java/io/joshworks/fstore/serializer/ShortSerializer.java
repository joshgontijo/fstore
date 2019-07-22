package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class ShortSerializer implements Serializer<Short> {

    @Override
    public void writeTo(Short data, ByteBuffer dst) {
        dst.putShort(data);
    }

    @Override
    public Short fromBytes(ByteBuffer data) {
        return data.getShort();
    }
}
