//package io.joshworks.es2.index.filter;
//
//import java.util.BitSet;
//
//public class BitSetArray {
//
//    private final BitSet bits;
//
//    public BitSetArray(BitSet bits) {
//        this.bits = bits;
//    }
//
//    public void set(long bitIdx) {
//        if (bitIdx > Integer.MAX_VALUE) {
//            throw new IllegalStateException("Index too big");
//        }
//        bits.set((int) bitIdx);
//    }
//
//    public boolean get(long bitIdx) {
//        if (bitIdx > Integer.MAX_VALUE) {
//            throw new IllegalStateException("Index too big");
//        }
//        return bits.get((int) bitIdx);
//    }
//
//    public long size() {
//        return bits.size();
//    }
//}
