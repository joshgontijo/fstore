package io.joshworks.fstore.serializer.arrays;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class IntegerArraySerializer implements Serializer<int[]> {

    @Override
    public void writeTo(int[] data, ByteBuffer dst) {
        dst.putInt(data.length);
        dst.asIntBuffer().put(data);
    }

    @Override
    public int[] fromBytes(ByteBuffer data) {
        int size = data.getInt();
        int[] array = new int[size];
        for (int i = 0; i < array.length; i++) {
            array[i] = data.getInt();
        }
        return array;
    }

}
