package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class BooleanSerializer implements Serializer<Boolean> {

    @Override
    public void writeTo(Boolean data, ByteBuffer dst) {
        dst.put((byte) (data ? 1 : 0));
    }

    @Override
    public Boolean fromBytes(ByteBuffer data) {
        return data.get() == (byte) 1;
    }
}
