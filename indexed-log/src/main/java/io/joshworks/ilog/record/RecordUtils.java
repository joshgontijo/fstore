//package io.joshworks.ilog.record;
//
//import io.joshworks.fstore.core.io.buffers.Buffers;
//import io.joshworks.fstore.core.util.ByteBufferChecksum;
//import io.joshworks.ilog.index.RowKey;
//
//import java.io.IOException;
//import java.nio.BufferOverflowException;
//import java.nio.ByteBuffer;
//import java.nio.channels.WritableByteChannel;
//
//@Deprecated
//class RecordUtils {
//
//    public static boolean hasAttribute(ByteBuffer buffer, int attribute) {
//        byte attr = buffer.get(buffer.position() + Record2.ATTRIBUTE_OFFSET);
//        return (attr & (1 << attribute)) == 1;
//    }
//
//    public static int compareRecordKeys(ByteBuffer r1, ByteBuffer r2, RowKey comparator) {
//        return comparator.compare(r1, Record2.KEY_OFFSET, r2, Record2.KEY_OFFSET);
//    }
//
//    public static int compareToKey(ByteBuffer record, ByteBuffer key, RowKey comparator) {
//        return comparator.compare(record, Record2.KEY_OFFSET, key, key.position());
//    }
//
//    public static int sizeOf(ByteBuffer record) {
//        return record.getInt(record.position() + Record2.RECORD_LEN_OFFSET);
//    }
//
//    public static int keySize(ByteBuffer record) {
//        int recSize = sizeOf(record);
//        int valSize = valueSize(record);
//
//        return recSize - Record2.HEADER_BYTES - valSize;
//    }
//
//    public static int valueSize(ByteBuffer record) {
//        return record.getInt(record.position() + Record2.VALUE_LEN_OFFSET);
//    }
//
//    public static int checksum(ByteBuffer record) {
//        return record.getInt(record.position() + Record2.CHECKSUM_OFFSET);
//    }
//
//    private static int valueOffset(ByteBuffer record) {
//        int recSize = sizeOf(record);
//        int valSize = valueSize(record);
//        return recSize - valSize;
//    }
//
//    @Deprecated
//    public static int create(ByteBuffer key, ByteBuffer value, ByteBuffer dst, int... attr) {
//        throw new UnsupportedOperationException();
//    }
//
//    public static int copyTo(ByteBuffer record, ByteBuffer dst) {
//
//        if (!isValid(record)) {
//            throw new RuntimeException("Invalid record");
//        }
//
//        int recLen = sizeOf(record);
//        if (dst.remaining() < recLen) {
//            throw new BufferOverflowException();
//        }
//
//        int copied = Buffers.copy(record, record.position(), recLen, dst);
//        assert copied == recLen;
//        return copied;
//    }
//
//    public static boolean isValid(ByteBuffer record) {
//        int remaining = record.remaining();
//        if (remaining < Record2.HEADER_BYTES) {
//            return false;
//        }
//        int recordSize = sizeOf(record);
//        if (recordSize > remaining) {
//            return false;
//        }
//        if (recordSize <= Record2.HEADER_BYTES) {
//            return false;
//        }
//
//        int valueSize = valueSize(record);
//
//        int keySize = keySize(record);
//        if (keySize <= 0 || keySize > recordSize - valueSize) {
//            return false;
//        }
//
//        if (sizeOf(record) != Record2.HEADER_BYTES + keySize + valueSize(record)) {
//            return false;
//        }
//
//        int checksum = checksum(record);
//        int valOffset = valueOffset(record);
//        int absValPos = Buffers.toAbsolutePosition(record, valOffset);
//        int computedChecksum = ByteBufferChecksum.crc32(record, absValPos, valueSize);
//        return computedChecksum == checksum;
//    }
//
//    public static int writeTo(ByteBuffer record, WritableByteChannel channel) throws IOException {
//        if (!RecordUtils.isValid(record)) {
//            throw new IllegalStateException("Invalid record");
//        }
//        int rsize = RecordUtils.sizeOf(record);
//        if (record.remaining() < rsize) {
//            return 0;
//        }
//        int plimit = record.limit();
//        record.limit(record.position() + rsize);
//        int written = channel.write(record);
//        record.limit(plimit);
//        return written;
//    }
//
//    private static byte attribute(int... attributes) {
//        byte b = 0;
//        for (int attr : attributes) {
//            b = (byte) (b | 1 << attr);
//        }
//        return b;
//    }
//}
