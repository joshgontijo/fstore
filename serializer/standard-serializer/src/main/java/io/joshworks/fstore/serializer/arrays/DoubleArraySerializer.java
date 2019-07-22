package io.joshworks.fstore.serializer.arrays;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class DoubleArraySerializer implements Serializer<double[]> {

    @Override
    public void writeTo(double[] data, ByteBuffer dst) {
        dst.putInt(data.length);
        dst.asDoubleBuffer().put(data);
    }

    @Override
    public double[] fromBytes(ByteBuffer data) {
        int size = data.getInt();
        double[] array = new double[size];
        for (int i = 0; i < array.length; i++) {
            array[i] = data.getDouble();
        }
        return array;
    }

}
