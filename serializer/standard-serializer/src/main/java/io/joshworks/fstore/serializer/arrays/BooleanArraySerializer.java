package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class BooleanArraySerializer extends SizePrefixedArraySerializer<boolean[]> {

    @Override
    public ByteBuffer toBytes(boolean[] data) {
        ByteBuffer bb = allocate(data.length);
        for (boolean aData : data) {
            bb.put((byte) (aData ? 1 : 0));
        }
        return bb.flip();
    }

    @Override
    public void writeTo(boolean[] data, ByteBuffer dst) {
        dst.putInt(data.length);
        for (boolean aData : data) {
            dst.put((byte) (aData ? 1 : 0));
        }
    }

    @Override
    public boolean[] fromBytes(ByteBuffer data) {
        int size = getSize(data);
        //TODO improve this
        boolean[] array = new boolean[size];
        for (int i = 0; i < array.length; i++) {
            array[i] = data.get() == (byte) 1;
        }
        return array;
    }

    @Override
    int byteSize() {
        return Byte.BYTES;
    }

}
