package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class FloatSerializer implements Serializer<Float> {

    @Override
    public void writeTo(Float data, ByteBuffer dst) {
        dst.putFloat(data);
    }

    @Override
    public Float fromBytes(ByteBuffer data) {
        return data.getFloat();
    }
}
