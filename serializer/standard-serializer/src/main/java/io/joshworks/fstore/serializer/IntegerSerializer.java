package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class IntegerSerializer implements Serializer<Integer> {

    @Override
    public void writeTo(Integer data, ByteBuffer dst) {
        dst.putInt(data);
    }

    @Override
    public Integer fromBytes(ByteBuffer data) {
        return data.getInt();
    }
}
