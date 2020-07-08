//package io.joshworks.ilog;
//
//import io.joshworks.fstore.core.io.buffers.Buffers;
//
//import java.nio.ByteBuffer;
//
//public class Records {
//
//    private final int[] lengths;
//    private final int[] offsets;
//    private int size;
//    private int baseOffset;
//    private int totalSize;
//
//    private int idx;
//
//    private ByteBuffer data;
//
//    public Records(int batchSize) {
//        this.lengths = new int[batchSize];
//        this.offsets = new int[batchSize];
//    }
//
//    public int parse(ByteBuffer data) {
//        this.data = data;
//        int baseOffset = data.position();
//        while(RecordBatch.hasNext(data)) {
//
//        }
//    }
//
//    public int size() {
//        return size;
//    }
//
//    public boolean isEmpty() {
//        return size() == 0;
//    }
//
//    public int index() {
//        return idx;
//    }
//
//    public void reset() {
//        idx = 0;
//    }
//
//    public int copy(int idx, ByteBuffer dst) {
//        return Buffers.copy(data, offsets[idx], lengths[idx], dst);
//    }
//
//    public int copyKey(int idx, ByteBuffer dst) {
//        return Buffers.copy(data, offsets[idx] + Record.KEY.offset(data), lengths[idx], dst);
//    }
//
//
//}
