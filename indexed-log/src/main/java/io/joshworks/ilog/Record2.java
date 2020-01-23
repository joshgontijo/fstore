package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.ChecksumException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.index.KeyComparator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

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
public class Record2 {

    private static final int DATA_LEN_LEN = Integer.BYTES;
    private static final int CHECKSUM_LEN = Integer.BYTES;
    private static final int TIMESTAMP_LEN = Long.BYTES;
    private static final int ATTR_LEN = Byte.BYTES;
    private static final int KEY_LEN_LEN = Integer.BYTES;

    public static final int HEADER_BYTES = DATA_LEN_LEN + CHECKSUM_LEN + TIMESTAMP_LEN + ATTR_LEN + KEY_LEN_LEN;

    public static final int DATA_LENGTH_OFFSET = 0;
    public static final int CHECKSUM_OFFSET = DATA_LENGTH_OFFSET + DATA_LEN_LEN;
    public static final int TIMESTAMP_OFFSET = CHECKSUM_OFFSET + CHECKSUM_LEN;
    public static final int ATTR_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LEN;
    public static final int KEY_LENGTH_OFFSET = ATTR_OFFSET + ATTR_LEN;
    public static final int KEY_OFFSET = KEY_LENGTH_OFFSET + KEY_LEN_LEN;


    public static int valueSize(ByteBuffer buffer) {
        return buffer.getInt(relativeFieldOffset(buffer, DATA_LENGTH_OFFSET));
    }

    public static int checksum(ByteBuffer buffer) {
        return buffer.getInt(relativeFieldOffset(buffer, CHECKSUM_OFFSET));
    }

    public static long timestamp(ByteBuffer buffer) {
        return buffer.getLong(relativeFieldOffset(buffer, TIMESTAMP_OFFSET));
    }

    public static int keySize(ByteBuffer buffer) {
        return buffer.getInt(relativeFieldOffset(buffer, KEY_LENGTH_OFFSET));
    }

    public static int size(ByteBuffer buffer) {
        return HEADER_BYTES + keySize(buffer) + valueSize(buffer);
    }

    public static boolean hasAttribute(ByteBuffer buffer, int attribute) {
        byte attr = buffer.get(relativeFieldOffset(buffer, ATTR_OFFSET));
        return (attr & (Byte.MAX_VALUE << attribute)) == 1;
    }

    public static int compareRecordKeys(ByteBuffer r1, ByteBuffer r2, KeyComparator comparator) {

        int r1p = r1.position();
        int r1l = r1.limit();

        Buffers.offsetPosition(r1, relativeFieldOffset(r1, KEY_OFFSET));
        Buffers.offsetLimit(r1, keySize(r1));

        int r2p = r2.position();
        int r2l = r2.limit();

        Buffers.offsetPosition(r2, relativeFieldOffset(r2, KEY_OFFSET));
        Buffers.offsetLimit(r2, keySize(r2));

        int compare = comparator.compare(r1, r2);

        r1.position(r1p).limit(r1l);
        r2.position(r2p).limit(r2l);

        return compare;
    }

    public static int compareToKey(ByteBuffer record, ByteBuffer key, KeyComparator comparator) {
        int rp = record.position();
        int rl = record.limit();

        Buffers.offsetPosition(record, relativeFieldOffset(record, KEY_OFFSET));
        Buffers.offsetLimit(record, keySize(record));

        int k2p = key.position();
        int k2l = key.limit();

        int compare = comparator.compare(record, key);

        record.position(rp).limit(rl);
        key.position(k2p).limit(k2l);

        return compare;
    }

    public static int computedSize(ByteBuffer key, ByteBuffer value) {
        return HEADER_BYTES + key.remaining() + value.remaining();
    }

    public static int sizeOf(ByteBuffer record) {
        int valSize = valueSize(record);
        int key = keySize(record);
        return HEADER_BYTES + valSize + key;
    }

    public static int writeKey(ByteBuffer record, ByteBuffer dst) {
        return Buffers.copy(record, relativeFieldOffset(record, KEY_OFFSET), keySize(record), dst);
    }

    public static int create(ByteBuffer key, ByteBuffer value, ByteBuffer dst) {
        if (dst.remaining() <= HEADER_BYTES) {
            throw new IllegalArgumentException("Write buffer must be at least " + HEADER_BYTES);
        }
        try {
            int recordStart = dst.position();
            int originalLimit = dst.limit();
            dst.position(HEADER_BYTES);
            int keyStart = dst.position();
            Buffers.copy(key, dst);
            int keyEnd = dst.position();

            int checksum = ByteBufferChecksum.crc32(value);
            Buffers.copy(value, dst);
            int dataEnd = dst.position();

            int keyLen = keyEnd - keyStart;
            int dataLen = dataEnd - keyEnd;

            dst.position(recordStart);

            dst.putInt(dataLen);
            dst.putInt(checksum);
            dst.putLong(System.currentTimeMillis());
            dst.put((byte) 0); //NO ATTRIBUTE TODO ?
            dst.putInt(keyLen);

            dst.position(dataEnd).limit(originalLimit);

            return dataEnd - recordStart;

        } catch (Exception e) {
            throw new RuntimeException("Failed to create record", e);
        }
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

        int valSize = valueSize(record);
        int valStart = valueStart(record);
        int computedChecksum = ByteBufferChecksum.crc32(record, valStart, valSize);
        if (computedChecksum != checksum(record)) {
            throw new ChecksumException();
        }
        return rsize;
    }

    public static int writeTo(ByteBuffer record, WritableByteChannel channel) throws IOException {
        int rsize = sizeOf(record);
        if (record.remaining() < rsize) {
            return 0;
        }
        int plimit = record.limit();
        record.limit(record.position() + rsize);
        int written = channel.write(record);
        record.limit(plimit);
        return written;
    }

    private static int valueStart(ByteBuffer buffer) {
        return HEADER_BYTES + keySize(buffer);
    }

    private static void readKey(ByteBuffer buffer, ByteBuffer dst) {
        int keyLen = keySize(buffer);
        Buffers.copy(buffer, HEADER_BYTES, keyLen, dst);
    }

    private static void readValue(ByteBuffer buffer, ByteBuffer dst) {
        int valueStart = HEADER_BYTES + keySize(buffer);
        int dataLen = buffer.limit() - valueStart;
        Buffers.copy(buffer, valueStart, dataLen, dst);
    }

    private static int relativeFieldOffset(ByteBuffer buffer, int fieldOffset) {
        return buffer.position() + fieldOffset;
    }

    public static String toString(ByteBuffer buffer) {
        return "Record{" +
                " recordLength=" + size(buffer) +
                ", checksum=" + checksum(buffer) +
                ", keyLength=" + keySize(buffer) +
                ", dataLength=" + valueSize(buffer) +
                ", timestamp=" + timestamp(buffer) +
                ", attributes=" + buffer.get(relativeFieldOffset(buffer, ATTR_OFFSET)) +
                '}';
    }

    public static <K, V> String toString(ByteBuffer buffer, ByteBuffer dst, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        readKey(buffer, dst);
        K k = keySerializer.fromBytes(dst.flip());

        dst.clear();

        readValue(buffer, dst);
        V v = valueSerializer.fromBytes(dst.flip());

        return "Record{" +
                " recordLength=" + size(buffer) +
                ", checksum=" + checksum(buffer) +
                ", keyLength=" + keySize(buffer) +
                ", dataLength=" + valueSize(buffer) +
                ", timestamp=" + timestamp(buffer) +
                ", attributes=" + buffer.get(relativeFieldOffset(buffer, ATTR_OFFSET)) +
                ", key=" + k +
                ", value=" + v +
                '}';
    }
}
