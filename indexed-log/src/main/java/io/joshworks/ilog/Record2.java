package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.ChecksumException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.index.KeyComparator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativeOffset;

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
        return buffer.getInt(relativeOffset(buffer, DATA_LENGTH_OFFSET));
    }

    public static int valueOffset(ByteBuffer buffer) {
        return relativeOffset(buffer, KEY_OFFSET) + keySize(buffer);
    }

    public static int checksum(ByteBuffer buffer) {
        return buffer.getInt(relativeOffset(buffer, CHECKSUM_OFFSET));
    }

    public static long timestamp(ByteBuffer buffer) {
        return buffer.getLong(relativeOffset(buffer, TIMESTAMP_OFFSET));
    }

    public static int keyOffset(ByteBuffer buffer) {
        return relativeOffset(buffer, KEY_OFFSET);
    }

    public static int keySize(ByteBuffer buffer) {
        return buffer.getInt(relativeOffset(buffer, KEY_LENGTH_OFFSET));
    }

    public static boolean hasAttribute(ByteBuffer buffer, int attribute) {
        byte attr = buffer.get(relativeOffset(buffer, ATTR_OFFSET));
        return (attr & (1 << attribute)) == 1;
    }

    public static int compareRecordKeys(ByteBuffer r1, ByteBuffer r2, KeyComparator comparator) {

        int r1p = r1.position();
        int r1l = r1.limit();

        int r1Offset = relativeOffset(r1, KEY_OFFSET);
        int k1Size = keySize(r1);
        Buffers.view(r1, r1Offset, k1Size);

        int r2p = r2.position();
        int r2l = r2.limit();

        int r2Offset = keyOffset(r2);
        int k2Size = keySize(r2);
        Buffers.view(r2, r2Offset, k2Size);

        int compare = comparator.compare(r1, r2);

        r1.position(r1p).limit(r1l);
        r2.position(r2p).limit(r2l);

        return compare;
    }

    public static int compareToKey(ByteBuffer record, ByteBuffer key, KeyComparator comparator) {
        int rp = record.position();
        int rl = record.limit();


        int offset = keyOffset(record);
        int keySize = keySize(record);
        Buffers.view(record, offset, keySize);

        int k2p = key.position();
        int k2l = key.limit();

        int compare = comparator.compare(record, key);

        record.position(rp).limit(rl);
        key.position(k2p).limit(k2l);

        return compare;
    }

    public static int sizeOf(ByteBuffer record) {
        int valSize = valueSize(record);
        int keySize = keySize(record);
        if (keySize == 0 && valSize == 0) {
            return 0;
        }
        return HEADER_BYTES + valSize + keySize;
    }

    public static int writeKey(ByteBuffer record, ByteBuffer dst) {
        int absKeyPos = keyOffset(record);
        return Buffers.copy(record, absKeyPos, keySize(record), dst);
    }

    public static int create(ByteBuffer key, ByteBuffer value, ByteBuffer dst, int... attributes) {
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
            dst.put(attribute(attributes));
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
        if (rsize <= HEADER_BYTES) {
            throw new RuntimeException("Invalid record");
        }

        int valSize = valueSize(record);
        int valStart = valueOffset(record);
        int computedChecksum = ByteBufferChecksum.crc32(record, valStart, valSize);
        if (computedChecksum != checksum(record)) {
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

    private static void readKey(ByteBuffer buffer, ByteBuffer dst) {
        int keyLen = keySize(buffer);
        Buffers.copy(buffer, HEADER_BYTES, keyLen, dst);
    }

    private static void readValue(ByteBuffer buffer, ByteBuffer dst) {
        int valueStart = HEADER_BYTES + keySize(buffer);
        int dataLen = buffer.limit() - valueStart;
        Buffers.copy(buffer, valueStart, dataLen, dst);
    }

    private static byte attribute(int... attributes) {
        byte b = 0;
        for (int attr : attributes) {
            b = (byte) (b | 1 << attr);
        }
        return b;
    }

    public static String toString(ByteBuffer buffer) {
        return "Record{" +
                " recordSize=" + sizeOf(buffer) +
                ", checksum=" + checksum(buffer) +
                ", keySize=" + keySize(buffer) +
                ", dataLength=" + valueSize(buffer) +
                ", timestamp=" + timestamp(buffer) +
                ", attributes=" + buffer.get(ATTR_OFFSET) +
                '}';
    }

    public static <K, V> String toString(ByteBuffer buffer, Serializer<K> ks, Serializer<V> vs) {
        int rsize = Record2.validate(buffer);
        ByteBuffer copy = Buffers.allocate(rsize, false);
        Buffers.copy(buffer, buffer.position(), rsize, copy);
        copy.flip();
        K k = Record2.readKey(copy, ks);
        V v = Record2.readValue(copy, vs);

        return "Record{" +
                " recordLength=" + Record2.sizeOf(buffer) +
                ", checksum=" + Record2.checksum(buffer) +
                ", keyLength=" + Record2.keySize(buffer) +
                ", dataLength=" + Record2.valueSize(buffer) +
                ", timestamp=" + Record2.timestamp(buffer) +
                ", key=" + k +
                ", value=" + v +
                '}';
    }

    //Testing only
    static <K> K readKey(ByteBuffer buffer, Serializer<K> ks) {
        int kPos = buffer.position() + KEY_OFFSET;
        int kSize = keySize(buffer);
        return ks.fromBytes(buffer.duplicate().position(kPos).limit(kPos + kSize));
    }

    //Testing only
    static <T> T readValue(ByteBuffer buffer, Serializer<T> ks) {
        int vPos = valueOffset(buffer);
        int vSize = valueSize(buffer);
        return ks.fromBytes(buffer.duplicate().limit(vPos + vSize).position(vPos));
    }
}
