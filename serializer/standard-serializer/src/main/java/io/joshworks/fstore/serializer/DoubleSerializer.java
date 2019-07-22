package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class DoubleSerializer implements Serializer<Double> {

    @Override
    public void writeTo(Double data, ByteBuffer dst) {
        dst.putDouble(data);
    }

    @Override
    public Double fromBytes(ByteBuffer data) {
        return data.getDouble();
    }
}
