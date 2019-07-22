package io.joshworks.fstore.serializer.arrays;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class FloatArraySerializer implements Serializer<float[]> {

    @Override
    public void writeTo(float[] data, ByteBuffer dst) {
        dst.putInt(data.length);
        dst.asFloatBuffer().put(data);
    }

    @Override
    public float[] fromBytes(ByteBuffer data) {
        int size = data.getInt();
        float[] array = new float[size];
        for (int i = 0; i < array.length; i++) {
            array[i] = data.getFloat();
        }
        return array;
    }

}
