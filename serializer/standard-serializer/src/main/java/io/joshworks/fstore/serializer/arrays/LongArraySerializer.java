package io.joshworks.fstore.serializer.arrays;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class LongArraySerializer implements Serializer<long[]> {

    @Override
    public void writeTo(long[] data, ByteBuffer dst) {
        dst.putInt(data.length);
        dst.asLongBuffer().put(data);
    }

    @Override
    public long[] fromBytes(ByteBuffer data) {
        int size = data.getInt();
        long[] array = new long[size];
        for (int i = 0; i < array.length; i++) {
            array[i] = data.getLong();
        }
        return array;
    }

}
