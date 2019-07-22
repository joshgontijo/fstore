package io.joshworks.fstore.serializer.arrays;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class ShortArraySerializer implements Serializer<short[]> {

    @Override
    public void writeTo(short[] data, ByteBuffer dst) {
        dst.putInt(data.length);
        dst.asShortBuffer().put(data);
    }

    @Override
    public short[] fromBytes(ByteBuffer data) {
        int size = data.getInt();
        short[] array = new short[size];
        for (int i = 0; i < array.length; i++) {
            array[i] = data.getShort();
        }
        return array;
    }
}
