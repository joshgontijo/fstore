//package io.joshworks.es2.index.filter;
//
//import java.nio.ByteBuffer;
//
//public class BufferBits {
//
//    private final ByteBuffer bits;
//
//    public BufferBits(ByteBuffer buffer) {
//        this.bits = buffer;
//    }
//
//    public void set(long bitIdx) {
//        if (bitIdx > Integer.MAX_VALUE) {
//            throw new IllegalStateException("Index too big");
//        }
//        int bpos = (int) (bitIdx / Byte.SIZE);
//        int vidx = (int) (bitIdx % Byte.SIZE);
//        byte v = bits.get(bpos);
//        v = (byte) ((v & 0xFF) | (Byte.MIN_VALUE >>> vidx));
//        bits.put(bpos, v);
//    }
//
//    public boolean get(long bitIdx) {
//        if (bitIdx > Integer.MAX_VALUE) {
//            throw new IllegalStateException("Index too big");
//        }
//        int bpos = (int) (bitIdx / Byte.SIZE);
//        int vidx = (int) (bitIdx % Byte.SIZE);
//        byte v = bits.get(bpos);
//        return ((Byte.MIN_VALUE >>> vidx) & (v & 0xFF)) != 0;
//    }
//
//    public long size() {
//        return bits.capacity();
//    }
//}
