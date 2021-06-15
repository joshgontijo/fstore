package io.joshworks.ilog.lsm.filter;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

public class BitArray {

    private static final long MAX_VAL = (((long) Buffers.MAX_CAPACITY) * Byte.SIZE);

    private final ByteBuffer bits;

    public BitArray(long size, boolean direct) {
        if (size > MAX_VAL) {
            throw new IllegalArgumentException("Buffer size cannot be greater than " + Integer.MAX_VALUE + ": m is too big");
        }

        int bsize = (int) ((size % Byte.SIZE == 0) ? (size / Byte.SIZE) : (size / Byte.SIZE) + 1);
        bits = Buffers.allocate(bsize, direct);
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
