package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;

import java.nio.ByteBuffer;

/**
 * <pre>
 * RECORD_LEN       (4 BYTES)
 * CHECKSUM         (4 BYTES)
 * -------------------
 * TIMESTAMP        (8 BYTES)
 * SEQUENCE         (8 BYTES)
 * ATTRIBUTES       (2 BYTES)
 * KEY_LEN          (2 BYTES)
 * VALUE_LEN        (4 BYTES)
 * [KEY]            (N BYTES)
 * [VALUE]          (N BYTES)
 * </pre>
 * <p>
 *
 * <pre>
 * All records starts at position zero
 * Checksum covers from TIMESTAMP inclusive
 * RECORD_LEN includes RECORD_LEN itself
 * KEY_LEN excludes KEY_LEN itself
 * VALUE_LEN excludes VALUE_LEN itself
 * </pre>
 */
public class Record {

    private ByteBuffer data;

    public static final int HEADER_BYTES =
            Integer.BYTES +         //RECORD_LEN
                    Integer.BYTES + //CHECKSUM
                    Long.BYTES +    //TIMESTAMP
                    Long.BYTES +    //SEQUENCE
                    Short.BYTES +   //ATTRIBUTES
                    Short.BYTES +   //KEY_LEN
                    Integer.BYTES;  //VALUE_LEN

    static final int RECORD_LEN_OFFSET = 0;
    static final int CHECKSUM_OFFSET = RECORD_LEN_OFFSET + Integer.BYTES;
    static final int TIMESTAMP_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    static final int SEQUENCE_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;
    static final int ATTRIBUTE_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    static final int KEY_LEN_OFFSET = ATTRIBUTE_OFFSET + Short.BYTES;
    static final int VALUE_LEN_OFFSET = KEY_LEN_OFFSET + Short.BYTES;
    static final int KEY_OFFSET = VALUE_LEN_OFFSET + Integer.BYTES;

    public static void advance(ByteBuffer data) {
        int size = size(data);
        if (data.remaining() < size) {
            throw new IndexOutOfBoundsException(data.position() + size);
        }
        Buffers.offsetPosition(data, size);
    }

    public static int totalSize(ByteBuffer data) {
        return totalSize(data, data.position());
    }

    public static int totalSize(ByteBuffer data, int offset) {
        return totalSize(data, offset, Integer.MAX_VALUE);
    }

    public static int totalSize(ByteBuffer data, int offset, int count) {
        int startOffset = offset;
        int entries = 0;
        while (isValid(data, offset) && entries < count) {
            offset += size(data, offset);
        }
        return offset - startOffset;
    }

    public static int entries(ByteBuffer data) {
        return entries(data, data.position());
    }

    public static int entries(ByteBuffer data, int offset) {
        int entries = 0;
        while (isValid(data, offset)) {
            entries++;
            offset += size(data, offset);
        }
        return entries;
    }

    public static int size(ByteBuffer data) {
        return size(data, data.position());
    }

    public static int size(ByteBuffer data, int offset) {
        return data.getInt(offset + RECORD_LEN_OFFSET);
    }

    public static int checksum(ByteBuffer data, int offset) {
        return data.getInt(offset + CHECKSUM_OFFSET);
    }

    public static int checksum(ByteBuffer data) {
        return checksum(data, data.position());
    }

    public static long timestamp(ByteBuffer data, int offset) {
        return data.getLong(offset + TIMESTAMP_OFFSET);
    }

    public static long timestamp(ByteBuffer data) {
        return timestamp(data, data.position());
    }

    public static long sequence(ByteBuffer data, int offset) {
        return data.getLong(offset + SEQUENCE_OFFSET);
    }

    public static long sequence(ByteBuffer data) {
        return sequence(data, data.position());
    }

    public static short keyLen(ByteBuffer data, int offset) {
        return data.getShort(offset + KEY_OFFSET);
    }

    public static short keyLen(ByteBuffer data) {
        return keyLen(data, data.position());
    }

    public static int valueLen(ByteBuffer data, int offset) {
        return data.getInt(offset + VALUE_LEN_OFFSET);
    }

    public static int valueLen(ByteBuffer data) {
        return valueLen(data, data.position());
    }

    public static int copyKey(ByteBuffer data, int dataOffset, ByteBuffer dst, int dstOffset) {
        int keyLen = keyLen(data, dataOffset);
        return Buffers.copy(data, dataOffset + KEY_OFFSET, keyLen, dst, dstOffset);
    }

    public static int copyKey(ByteBuffer data, int dataOffset, ByteBuffer dst) {
        return copyKey(data, dataOffset, dst, dst.position());
    }

    public static int copyKey(ByteBuffer data, ByteBuffer dst) {
        return copyKey(data, dst.position(), dst, dst.position());
    }

    public static int copyValue(ByteBuffer data, int dataOffset, ByteBuffer dst, int dstOffset) {
        int valOffset = valueOffset(data, dataOffset);
        int valLen = valueLen(data, dataOffset);
        return Buffers.copy(data, dataOffset + valOffset, valLen, dst, dstOffset);
    }

    public static int copyValue(ByteBuffer data, int dataOffset, ByteBuffer dst) {
        return copyValue(data, dataOffset, dst, dst.position());
    }

    public static int copyValue(ByteBuffer data, ByteBuffer dst) {
        return copyValue(data, data.position(), dst, dst.position());
    }

    public static int valueOffset(ByteBuffer data, int offset) {
        return offset + KEY_OFFSET + keyLen(data, offset);
    }

    public static int valueOffset(ByteBuffer data) {
        return valueOffset(data, data.position());
    }

    public static boolean isValid(ByteBuffer data) {
        return isValid(data, data.position());
    }

    public static boolean isValid(ByteBuffer data, int offset) {
        if (!canReadSizeField(data, offset)) {
            return false;
        }

        int recSize = size(data, offset);
        if (recSize <= 0 || recSize > Buffers.remaining(data, offset)) {
            return false;
        }

        int checksum = checksum(data, offset);
        int computed = computeChecksum(data, offset);

        return computed == checksum;
    }

    private static int computeChecksum(ByteBuffer data, int offset) {
        int chksOffset = offset + TIMESTAMP_OFFSET;
        int size = size(data, offset);
        int chksLen = size - (chksOffset - offset);
        return ByteBufferChecksum.crc32(data, chksOffset, chksLen);
    }

    private static boolean canReadSizeField(ByteBuffer recData, int offset) {
        return Buffers.remaining(recData, offset) >= Integer.BYTES;
    }

    public static boolean hasAttribute(ByteBuffer data, int attribute) {
        return hasAttribute(data, data.position(), attribute);
    }

    public static boolean hasAttribute(ByteBuffer data, int offset, int attribute) {
        short attr = attributes(data, offset);
        return (attr & (1 << attribute)) == 1;
    }

    public static short attributes(ByteBuffer data) {
        return attributes(data, data.position());
    }

    public static short attributes(ByteBuffer data, int offset) {
        return data.getShort(offset + ATTRIBUTE_OFFSET);
    }

    public static void writeSequence(ByteBuffer buffer, int offset, long sequence) {
        buffer.putLong(offset + SEQUENCE_OFFSET, sequence);
    }

    public static void writeSequence(ByteBuffer buffer, long sequence) {
        writeSequence(buffer, buffer.position(), sequence);
    }

    public static int create(ByteBuffer dst, ByteBuffer key, ByteBuffer value, int... attr) {
        int recordSize = computeRecordSize(key.remaining(), value.remaining()); //includes the RECORD_LEN field
        if (recordSize > dst.remaining()) {
            throw new IllegalArgumentException("Not enough buffer space to create record");
        }
        if (key.remaining() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Key length exceeds mac size of: " + Short.MAX_VALUE);
        }

        dst.putInt(recordSize);                     // RECORD_LEN
        dst.putInt(0);                              // CHECKSUM (tmp)
        dst.putLong(System.currentTimeMillis());    // TIMESTAMP
        dst.putLong(0);                             // SEQUENCE (tmp)
        dst.putShort(attribute(attr));              // ATTRIBUTES
        dst.putShort((short) key.remaining());      // KEY_LEN
        dst.putInt(value.remaining());              // VALUE_LEN
        Buffers.copy(key, dst);                     // [KEY]
        Buffers.copy(value, dst);                   // [VALUE]

        dst.flip();

        assert dst.remaining() == recordSize;
        assert size(dst) == recordSize;

        int checksum = ByteBufferChecksum.crc32(dst, TIMESTAMP_OFFSET, dst.remaining() - (Integer.BYTES * 2));
        dst.putInt(CHECKSUM_OFFSET, checksum);

        return recordSize;
    }

    public static int computeRecordSize(int kLen, int vLen) {
        return HEADER_BYTES + kLen + vLen;
    }

    private static short attribute(int... attributes) {
        short b = 0;
        for (int attr : attributes) {
            b = (short) (b | 1 << attr);
        }
        return b;
    }
}
