//package io.joshworks.ilog.record;
//
//import java.nio.ByteBuffer;
//
//// * RECORD_LEN       (4 BYTES)
//// * CHECKSUM         (4 BYTES)
//// * -------------------
//// * TIMESTAMP        (8 BYTES)
//// * SEQUENCE         (8 BYTES)
//// * ATTRIBUTES       (2 BYTES)
//// * KEY_LEN          (2 BYTES)
//// * VALUE_LEN        (4 BYTES)
//// * [KEY]            (N BYTES)
//// * [VALUE]          (N BYTES)
//public class HeapRecord {
//
//    private long timestamp;
//    private long sequence;
//    private short attributes;
//
//    private ByteBuffer key;
//    private ByteBuffer value;
//
//    public static HeapRecord create(long sequence, ByteBuffer key, ByteBuffer value) {
//        HeapRecord rec = new HeapRecord();
//        rec.timestamp = System.currentTimeMillis();
//
//        return rec;
//    }
//
//    public int size() {
//        return Record.HEADER_BYTES + key.remaining() + value.remaining();
//    }
//
//
//}
