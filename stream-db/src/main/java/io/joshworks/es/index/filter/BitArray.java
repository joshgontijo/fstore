package io.joshworks.es.index.filter;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

public class BitArray {

    private final ByteBuffer bits;

    public BitArray(ByteBuffer buffer) {
        this.bits = buffer;
    }


    public void set(long bitIdx) {
        if (bitIdx > Integer.MAX_VALUE) {
            throw new IllegalStateException("Index too big");
        }
        int bpos = (int) (bitIdx / Byte.SIZE);
        int vidx = (int) (bitIdx % Byte.SIZE);
        byte v = bits.get(bpos);
        v = (byte) (v | (Byte.MIN_VALUE >>> vidx));
        bits.put(bpos, v);
    }

    public boolean get(long bitIdx) {
        if (bitIdx > Integer.MAX_VALUE) {
            throw new IllegalStateException("Index too big");
        }
        int bpos = (int) (bitIdx / Byte.SIZE);
        int vidx = (int) (bitIdx % Byte.SIZE);
        byte v = bits.get(bpos);
        return ((Byte.MIN_VALUE >>> vidx) & v) != 0;
    }

    public long size() {
        return bits.capacity();
    }
}
