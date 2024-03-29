package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class LongSerializer implements Serializer<Long> {

    @Override
    public void writeTo(Long data, ByteBuffer dst) {
        dst.putLong(data);
    }

    @Override
    public Long fromBytes(ByteBuffer data) {
        return data.getLong();
    }
}
