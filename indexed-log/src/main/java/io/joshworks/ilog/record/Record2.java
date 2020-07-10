package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.index.RowKey;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * <pre>
 * RECORD_LEN       (4 BYTES)
 * CHECKSUM         (4 BYTES)
 * -------------------
 * TIMESTAMP        (8 BYTES)
 * SEQUENCE         (8 BYTES)
 * ATTRIBUTES       (2 BYTES)
 * KEY_LEN          (2 BYTES)
 * [KEY]            (N BYTES)
 * VALUE_LEN        (4 BYTES)
 * [VALUE]          (N BYTES)
 * </pre>
 * <p>
 *
 * <pre>
 * All records starts at position zero
 * Checksum covers from TIMESTAMP inclusive
 * RECORD_LEN excludes RECORD_LEN itself
 * KEY_LEN excludes KEY_LEN itself
 * VALUE_LEN excludes VALUE_LEN itself
 * </pre>
 */
public class Record2 {

    private ByteBuffer data;

    private static final int HEADER_BYTES =
            Integer.BYTES +         //RECORD_LEN
                    Integer.BYTES + //CHECKSUM
                    Long.BYTES +    //TIMESTAMP
                    Long.BYTES +    //SEQUENCE
                    Short.BYTES +   //ATTRIBUTES
                    Short.BYTES +   //KEY_LEN
                    Integer.BYTES;  //VALUE_LEN

    public static final int RECORD_LEN_OFFSET = 0;
    public static final int CHECKSUM_OFFSET = RECORD_LEN_OFFSET + Integer.BYTES;
    public static final int TIMESTAMP_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    public static final int SEQUENCE_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;
    public static final int ATTRIBUTE_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    public static final int KEY_LEN_OFFSET = ATTRIBUTE_OFFSET + Short.BYTES;
    public static final int KEY_OFFSET = KEY_LEN_OFFSET + Short.BYTES;

    boolean active;

    Record2() {
    }

    void free() {
        this.data = null;
        this.active = false;
    }

    boolean parse(ByteBuffer data) {
        assert !active : "Record is active";

        if (data.remaining() < HEADER_BYTES) {
            return false;
        }

        int base = data.position();

        int recLen = data.getInt(base + RECORD_LEN_OFFSET);
        int checksum = data.getInt(base + CHECKSUM_OFFSET);

        if (recLen < 0 || recLen > data.remaining()) {
            return false;
        }

        int chksOffset = base + TIMESTAMP_OFFSET; //from TIMESTAMP
        int chksLen = recLen - (Integer.BYTES * 2);

        int computed = ByteBufferChecksum.crc32(data, chksOffset, chksLen);

        return active = computed == checksum;
    }

    public boolean hasAttribute(ByteBuffer buffer, int attribute) {
        short attr = buffer.getShort(ATTRIBUTE_OFFSET);
        return (attr & (1 << attribute)) == 1;
    }

    public int recordSize() {
        return data.getInt(RECORD_LEN_OFFSET);
    }

    public long sequence() {
        return data.getInt(SEQUENCE_OFFSET);
    }

    public int keySize() {
        return data.getInt(KEY_LEN_OFFSET);
    }

    private int valueLenOffset() {
        return KEY_LEN_OFFSET + keySize();
    }

    private int valueOffset() {
        return valueLenOffset() + Integer.BYTES;
    }

    public int valueSize() {
        return data.getInt(valueLenOffset());
    }

    public int checksum() {
        return data.getInt(CHECKSUM_OFFSET);
    }

    public void writeSequence(long sequence) {
        data.putLong(SEQUENCE_OFFSET, sequence);
    }

    public static Record2 create(long sequence, ByteBuffer key, ByteBuffer value, int... attr) {
        int recordSize =
                (HEADER_BYTES - Integer.BYTES) //excludes RECORD_LEN
                        + key.remaining()
                        + value.remaining();

        ByteBuffer dst = Buffers.allocate(recordSize, false);

        dst.putInt(recordSize);                     // RECORD_LEN
        dst.putInt(0);                              // CHECKSUM
        dst.putLong(System.currentTimeMillis());    // TIMESTAMP
        dst.putLong(sequence);                      // SEQUENCE
        dst.putShort(attribute(attr));              // ATTRIBUTES
        dst.putShort((short) key.remaining());      // KEY_LEN
        Buffers.copy(key, dst);                     // [KEY]
        dst.putInt(value.remaining());              // VALUE_LEN
        Buffers.copy(value, dst);                   // [VALUE]

        dst.flip();

        assert dst.remaining() == recordSize - Integer.BYTES;

        int checksum = ByteBufferChecksum.crc32(dst, TIMESTAMP_OFFSET, dst.remaining() - (Integer.BYTES * 2));
        dst.putInt(CHECKSUM_OFFSET, checksum);

        Record2 rec = new Record2();
        rec.data = dst;

        return rec;
    }

    public int copyTo(ByteBuffer dst) {
        int recLen = recordSize();
        if (dst.remaining() < recLen) {
            throw new BufferOverflowException();
        }

        int copied = Buffers.copy(data, 0, recLen, dst);
        assert copied == recLen;
        return copied;
    }

    public int writeTo(WritableByteChannel channel) throws IOException {
        int rsize = recordSize();
        if (data.remaining() < rsize) {
            return 0;
        }
        data.position(0).limit(rsize);
        return channel.write(data);
    }

    private static short attribute(int... attributes) {
        short b = 0;
        for (int attr : attributes) {
            b = (short) (b | 1 << attr);
        }
        return b;
    }

    public int copyKey(ByteBuffer dst) {
        return Buffers.copy(data, KEY_OFFSET, keySize(), dst);
    }

    public int writeValue(ByteBuffer dst) {
        return Buffers.copy(data, valueOffset(), valueSize(), dst);
    }

    public void copyValue(ByteBuffer dst) {
        Buffers.copy(data, valueOffset(), valueSize(), dst);
    }

    //TODO remove RowKey ?
    public int compare(RowKey rowKey, ByteBuffer key) {
        int keySize = rowKey.keySize();
        int r1ks = keySize();
        if (keySize != r1ks) {
            throw new IllegalStateException("Invalid key size: " + r1ks + " expected: " + keySize);
        }
        return Buffers.compare(data, KEY_OFFSET, key, key.position(), rowKey.keySize());
    }

    //TODO remove RowKey ?
    public static int compare(RowKey rowKey, Record2 r1, Record2 r2) {
        int keySize = rowKey.keySize();
        int r1ks = r1.keySize();
        int r2ks = r2.keySize();
        if (keySize != r1ks) {
            throw new IllegalStateException("Invalid key size: " + r1ks + " expected: " + keySize);
        }
        if (keySize != r2ks) {
            throw new IllegalStateException("Invalid key size: " + r2ks + " expected: " + keySize);
        }

        return Buffers.compare(r1.data, KEY_OFFSET, r2.data, KEY_OFFSET, keySize);
    }

}
