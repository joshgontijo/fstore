package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.index.RowKey;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * <pre>
 * ----- HEADER -----
 * RECORD_LEN (4 BYTES)
 * VALUE_LEN (4 BYTES)
 * CHECKSUM (4 BYTES)
 * TIMESTAMP (8 BYTES)
 * ATTR (1 BYTES)
 *
 * ----- KEY_VALUE -----
 * [KEY] (N BYTES)
 * [VALUE] (N BYTES)
 * </pre>
 */
public class Record {

    public static final int HEADER_BYTES = (Integer.BYTES * 3) + Long.BYTES + Byte.BYTES;

    private static final int RECORD_LEN_OFFSET = 0;
    private static final int VALUE_LEN_OFFSET = RECORD_LEN_OFFSET + Integer.BYTES;
    private static final int CHECKSUM_OFFSET = VALUE_LEN_OFFSET + Integer.BYTES;
    private static final int TIMESTAMP_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    private static final int ATTRIBUTE_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;

    private static final int KEY_OFFSET = ATTRIBUTE_OFFSET + Byte.BYTES;

    public static boolean hasAttribute(ByteBuffer buffer, int attribute) {
        byte attr = buffer.get(buffer.position() + ATTRIBUTE_OFFSET);
        return (attr & (1 << attribute)) == 1;
    }


    public static int compareRecordKeys(ByteBuffer r1, ByteBuffer r2, RowKey comparator) {
        return comparator.compare(r1, KEY_OFFSET, r2, KEY_OFFSET);
    }

    public static int compareToKey(ByteBuffer record, ByteBuffer key, RowKey comparator) {
        return comparator.compare(record, KEY_OFFSET, key, key.position());
    }

    public static int sizeOf(ByteBuffer record) {
        return record.getInt(record.position() + RECORD_LEN_OFFSET);
    }

    public static int keySize(ByteBuffer record) {
        if (isValid(record)) {
            throw new RuntimeException("Invalid record");
        }
        int recSize = sizeOf(record);
        int valSize = valueSize(record);

        return recSize - HEADER_BYTES - valSize;
    }

    public static int valueSize(ByteBuffer record) {
        return record.getInt(record.position() + VALUE_LEN_OFFSET);
    }

    public static int checksum(ByteBuffer record) {
        return record.getInt(record.position() + CHECKSUM_OFFSET);
    }

    private static int valueOffset(ByteBuffer record) {
        int recSize = sizeOf(record);
        int valSize = valueSize(record);
        return recSize - valSize;
    }

    public static int create(ByteBuffer key, ByteBuffer value, ByteBuffer dst, int... attr) {
        int checksum = ByteBufferChecksum.crc32(value);

        int recordStart = dst.position();

        int recLen = 0;
        dst.putInt(HEADER_BYTES + key.remaining() + value.remaining()); // RECORD_LEN
        dst.putInt(value.remaining()); // VALUE_LEN
        dst.putInt(checksum); // CHECKSUM
        dst.putLong(System.currentTimeMillis()); // TIMESTAMP
        dst.put(attribute(attr)); // ATTRIBUTES

        Buffers.copy(key, dst);
        Buffers.copy(value, dst);

        int recordEnd = dst.position();

        dst.position(recordStart);
        if (!isValid(dst)) {
            throw new RuntimeException("Invalid record");
        }
        dst.position(recordEnd);

        return recLen;
    }

    public static int copyTo(ByteBuffer record, ByteBuffer dst) {

        if (!isValid(record)) {
            throw new RuntimeException("Invalid record");
        }

        int recLen = sizeOf(record);
        if (dst.remaining() < recLen) {
            throw new BufferOverflowException();
        }

        int copied = Buffers.copy(record, record.position(), recLen, dst);
        assert copied == recLen;
        return copied;
    }

    public static boolean isValid(ByteBuffer record) {
        int remaining = record.remaining();
        if (remaining < HEADER_BYTES) {
            return false;
        }
        int recordSize = sizeOf(record);
        if (recordSize > remaining) {
            return false;
        }
        if (recordSize <= HEADER_BYTES) {
            return false;
        }

        int valueSize = valueSize(record);

        int keySize = keySize(record);
        if (keySize <= 0 || keySize > recordSize - valueSize) {
            return false;
        }

        if (sizeOf(record) != HEADER_BYTES + keySize + valueOffset(record)) {
            return false;
        }

        int checksum = checksum(record);
        int valOffset = valueOffset(record);
        int absValPos = Buffers.toAbsolutePosition(record, valOffset);
        int computedChecksum = ByteBufferChecksum.crc32(record, absValPos, valueSize);
        return computedChecksum == checksum;
    }

    public static int writeTo(ByteBuffer record, WritableByteChannel channel) throws IOException {
        if (!Record.isValid(record)) {
            throw new IllegalStateException("Invalid record");
        }
        int rsize = Record.sizeOf(record);
        if (record.remaining() < rsize) {
            return 0;
        }
        int plimit = record.limit();
        record.limit(record.position() + rsize);
        int written = channel.write(record);
        record.limit(plimit);
        return written;
    }

    private static byte attribute(int... attributes) {
        byte b = 0;
        for (int attr : attributes) {
            b = (byte) (b | 1 << attr);
        }
        return b;
    }
}
