package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.index.RowKey;

import java.io.Closeable;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class Record2 implements Comparable<Record2>, Closeable {

    ByteBuffer data;
    final Records owner;
    private final RowKey rowKey;

    public static final int HEADER_BYTES = (Integer.BYTES * 3) + (Long.BYTES * 2) + Byte.BYTES;

    public static final int RECORD_LEN_OFFSET = 0;
    public static final int VALUE_LEN_OFFSET = RECORD_LEN_OFFSET + Integer.BYTES;
    public static final int SEQUENCE_OFFSET = VALUE_LEN_OFFSET + Integer.BYTES;
    public static final int CHECKSUM_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    public static final int TIMESTAMP_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    public static final int ATTRIBUTE_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;

    public static final int KEY_OFFSET = ATTRIBUTE_OFFSET + Byte.BYTES;

    Record2(Records owner, RowKey rowKey) {
        this.owner = owner;
        this.rowKey = rowKey;
    }

    public boolean hasAttribute(ByteBuffer buffer, int attribute) {
        byte attr = buffer.get(buffer.position() + ATTRIBUTE_OFFSET);
        return (attr & (1 << attribute)) == 1;
    }

    public int recordSize() {
        return data.getInt(RECORD_LEN_OFFSET);
    }

    public int keySize() {
        return rowKey.keySize();
    }

    public int valueSize() {
        return data.getInt(data.position() + VALUE_LEN_OFFSET);
    }

    public int checksum() {
        return data.getInt(data.position() + CHECKSUM_OFFSET);
    }

    int valueOffset() {
        int recSize = recordSize();
        int valSize = valueSize();
        return recSize - valSize;
    }

    public static Record2 create(long sequence, RowKey rowKey, ByteBuffer key, ByteBuffer value, int... attr) {

        int checksum = ByteBufferChecksum.crc32(value);
        int recordSize = HEADER_BYTES + key.remaining() + value.remaining();

        ByteBuffer dst = Buffers.allocate(recordSize, false);

        dst.putInt(recordSize); // RECORD_LEN (including this field)
        dst.putInt(value.remaining()); // VALUE_LEN
        dst.putLong(sequence); // SEQUENCE
        dst.putInt(checksum); // CHECKSUM
        dst.putLong(System.currentTimeMillis()); // TIMESTAMP
        dst.put(attribute(attr)); // ATTRIBUTES

        Buffers.copy(key, dst);
        Buffers.copy(value, dst);
        dst.flip();

        Record2 rec = new Record2(null, rowKey);
        rec.data = dst;

        return rec;
    }

    public static void writeHeader(ByteBuffer dst, int keyLen, long sequence, int valLen, int checksum, int... attr) {
        dst.putInt(HEADER_BYTES + keyLen + valLen); // RECORD_LEN (including this field)
        dst.putInt(valLen); // VALUE_LEN
        dst.putLong(sequence); // SEQUENCE
        dst.putInt(checksum); // CHECKSUM
        dst.putLong(System.currentTimeMillis()); // TIMESTAMP
        dst.put(attribute(attr)); // ATTRIBUTES
    }

    public int copyTo(ByteBuffer dst) {
        int recLen = recordSize();
        if (dst.remaining() < recLen) {
            throw new BufferOverflowException();
        }

        int copied = Buffers.copy(data, data.position(), recLen, dst);
        assert copied == recLen;
        return copied;
    }

    public int writeTo(WritableByteChannel channel) throws IOException {
        int rsize = recordSize();
        if (data.remaining() < rsize) {
            return 0;
        }
        int plimit = data.limit();
        data.limit(data.position() + rsize);
        int written = channel.write(data);
        data.limit(plimit);
        return written;
    }

    private static byte attribute(int... attributes) {
        byte b = 0;
        for (int attr : attributes) {
            b = (byte) (b | 1 << attr);
        }
        return b;
    }

    public int copyKey(ByteBuffer dst) {
        return Buffers.copy(data, KEY_OFFSET, keySize(), dst);
    }

    public int writeValue(ByteBuffer dst) {
        return Buffers.copy(data, valueOffset(), valueSize(), dst);
    }

    public int compare(ByteBuffer key) {
        return rowKey.compare(data, KEY_OFFSET, key, key.position());
    }

    public boolean valid() {
        return data != null;
    }

    @Override
    public int compareTo(Record2 o) {
        if (!o.rowKey.getClass().equals(rowKey.getClass())) {
            throw new IllegalArgumentException("Incompatible records");
        }
        return rowKey.compare(data, KEY_OFFSET, o.data, KEY_OFFSET);
    }

    @Override
    public void close() {
        if (owner != null) {
            owner.free(this);
        }
    }

    public void copyValue(ByteBuffer dst) {
        Buffers.copy(data, valueOffset(), valueSize(), dst);
    }
}
