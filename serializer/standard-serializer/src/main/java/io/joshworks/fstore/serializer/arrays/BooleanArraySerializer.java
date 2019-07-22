package io.joshworks.fstore.serializer.arrays;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

//TODO improve by packing into long
public class BooleanArraySerializer implements Serializer<boolean[]> {

    @Override
    public void writeTo(boolean[] data, ByteBuffer dst) {
        dst.putInt(data.length);
        for (boolean aData : data) {
            dst.put((byte) (aData ? 1 : 0));
        }
    }

    @Override
    public boolean[] fromBytes(ByteBuffer data) {
        int size = data.getInt();
        boolean[] array = new boolean[size];
        for (int i = 0; i < array.length; i++) {
            array[i] = data.get() == (byte) 1;
        }
        return array;
    }

}
