package io.joshworks.ilog;

import io.joshworks.fstore.core.io.ChecksumException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.fields.BlobField;
import io.joshworks.ilog.fields.ByteField;
import io.joshworks.ilog.fields.IntField;
import io.joshworks.ilog.fields.LongField;
import io.joshworks.ilog.index.KeyComparator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

/**
 * VALUE_LEN (4 BYTES)
 * CHECKSUM (4 BYTES)
 * TIMESTAMP (8 BYTES)
 * ATTR (1 BYTES)
 * KEY_LEN (4 BYTES)
 * <p>
 * [KEY] (N BYTES)
 * [VALUE] (N BYTES)
 */
public class Record {

    public static final int HEADER_BYTES = (Integer.BYTES * 3) + Long.BYTES + Byte.BYTES;

    public static final IntField VALUE_LEN = new IntField(0);
    public static final IntField CHECKSUM = new IntField(4);
    public static final LongField TIMESTAMP = new LongField(8);
    public static final ByteField ATTRIBUTE = new ByteField(16);
    public static final IntField KEY_LEN = new IntField(17);
    public static final BlobField KEY = new BlobField(21, KEY_LEN::get);
    public static final BlobField VALUE = BlobField.after(KEY, VALUE_LEN::get);


    public static boolean hasAttribute(ByteBuffer buffer, int attribute) {
        byte attr = ATTRIBUTE.get(buffer);
        return (attr & (1 << attribute)) == 1;
    }

    public static int compareRecordKeys(ByteBuffer r1, ByteBuffer r2, KeyComparator comparator) {
        int k1Offset = KEY.offset(r1);
        int k2Offset = KEY.offset(r2);
        return comparator.compare(r1, k1Offset, r2, k2Offset);
    }

    public static int compareToKey(ByteBuffer record, ByteBuffer key, KeyComparator comparator) {
        int k1Offset = KEY.offset(record);
        return comparator.compare(record, k1Offset, key, key.position());
    }

    public static int sizeOf(ByteBuffer record) {
        int valSize = VALUE_LEN.get(record);
        int keySize = KEY_LEN.get(record);
        if (keySize == 0 && valSize == 0) {
            return 0;
        }
        return HEADER_BYTES + valSize + keySize;
    }

    public static int create(ByteBuffer key, ByteBuffer value, ByteBuffer dst, int... attr) {
        int keyLen = key.remaining();
        int valueLen = value.remaining();
        int checksum = ByteBufferChecksum.crc32(value);

        int recLen = 0;
        recLen += VALUE_LEN.set(dst, valueLen);
        recLen += CHECKSUM.set(dst, checksum);
        recLen += TIMESTAMP.set(dst, System.currentTimeMillis());
        recLen += ATTRIBUTE.set(dst, attribute(attr));
        recLen += KEY_LEN.set(dst, keyLen);
        recLen += KEY.set(dst, key);
        recLen += VALUE.set(dst, value);

        Buffers.offsetPosition(dst, recLen);
        return recLen;
    }

    public static int copyTo(ByteBuffer record, ByteBuffer dst) {

        assert Record.isValid(record);

        int ppos = dst.position();

        int recLen = 0;
        recLen += VALUE_LEN.copyTo(record, dst);
        recLen += CHECKSUM.copyTo(record, dst);
        recLen += TIMESTAMP.copyTo(record, dst);
        recLen += ATTRIBUTE.copyTo(record, dst);
        recLen += KEY_LEN.copyTo(record, dst);
        recLen += KEY.copyTo(record, dst);
        recLen += VALUE.copyTo(record, dst);

        int recordEnd = dst.position();

        dst.position(ppos);
        assert Record.isValid(dst);
        dst.position(recordEnd);

        return recLen;
    }

    public static boolean isValid(ByteBuffer record) {
        int remaining = record.remaining();
        if (remaining < HEADER_BYTES) {
            return false;
        }
        int rsize = sizeOf(record);
        if (rsize > remaining) {
            return false;
        }
        if (rsize <= HEADER_BYTES) {
            return false;
        }

        int valSize = VALUE_LEN.get(record);
        int valOffset = VALUE.offset(record);
        int checksum = CHECKSUM.get(record);

        int absValPos = relativePosition(record, valOffset);
        int computedChecksum = ByteBufferChecksum.crc32(record, absValPos, valSize);
        return computedChecksum == checksum;
    }

    public static int validate(ByteBuffer record) {
        int remaining = record.remaining();
        if (remaining < HEADER_BYTES) {
            throw new RuntimeException("Invalid record");
        }
        int rsize = sizeOf(record);
        if (rsize > remaining) {
            throw new RuntimeException("Invalid record");
        }
        if (rsize <= HEADER_BYTES) {
            throw new RuntimeException("Invalid record");
        }

        int valSize = VALUE_LEN.get(record);
        int valOffset = VALUE.offset(record);
        int checksum = CHECKSUM.get(record);

        int absValPos = relativePosition(record, valOffset);
        int computedChecksum = ByteBufferChecksum.crc32(record, absValPos, valSize);
        if (computedChecksum != checksum) {
            throw new ChecksumException();
        }
        return rsize;
    }

    public static int writeTo(ByteBuffer record, WritableByteChannel channel) throws IOException {
        int rsize = validate(record);
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

    public static String toString(ByteBuffer buffer) {
        int keySize = KEY_LEN.get(buffer);
        String key = keySize <= Long.BYTES ? "" + buffer.getLong(relativePosition(buffer, KEY.offset(buffer))) : "[BINARY]";

        return "Record{" +
                " recordSize=" + sizeOf(buffer) +
                " key=" + key +
                ", checksum=" + CHECKSUM.get(buffer) +
                ", keySize=" + keySize +
                ", dataLength=" + VALUE_LEN.get(buffer) +
                ", timestamp=" + TIMESTAMP.get(buffer) +
                ", attributes=" + Integer.toBinaryString(ATTRIBUTE.get(buffer)) +
                '}';
    }

}
