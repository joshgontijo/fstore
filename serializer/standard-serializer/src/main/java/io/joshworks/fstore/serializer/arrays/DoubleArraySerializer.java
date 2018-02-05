package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class DoubleArraySerializer extends ArraySerializer<double[]> {

    @Override
    public ByteBuffer toBytes(double[] data) {
        ByteBuffer bb = allocate(data.length);
        bb.asDoubleBuffer().put(data);
        bb.clear();
        return bb;
    }

    @Override
    public double[] fromBytes(ByteBuffer data) {
        int size = getSize(data);
        double[] array = new double[size];
        data.asDoubleBuffer().get(array);
        return array;
    }

    @Override
    int byteSize() {
        return Double.BYTES;
    }

}
