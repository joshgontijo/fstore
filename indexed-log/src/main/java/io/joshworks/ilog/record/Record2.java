package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.RowKey;

import java.io.Closeable;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class Record2 implements Comparable<Record2>, Closeable {

    ByteBuffer data;
    final BufferRecords owner;
    private final RowKey rowKey;

    public static final int HEADER_BYTES = (Integer.BYTES * 3) + (Long.BYTES * 2) + Byte.BYTES;

    private static final int RECORD_LEN_OFFSET = 0;
    static final int VALUE_LEN_OFFSET = RECORD_LEN_OFFSET + Integer.BYTES;
    static final int SEQUENCE_OFFSET = VALUE_LEN_OFFSET + Integer.BYTES;
    private static final int CHECKSUM_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    private static final int TIMESTAMP_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    private static final int ATTRIBUTE_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;

    static final int KEY_OFFSET = ATTRIBUTE_OFFSET + Byte.BYTES;

    Record2(BufferRecords owner, RowKey rowKey) {
        this.owner = owner;
        this.rowKey = rowKey;
    }

    public boolean hasAttribute(ByteBuffer buffer, int attribute) {
        byte attr = buffer.get(buffer.position() + ATTRIBUTE_OFFSET);
        return (attr & (1 << attribute)) == 1;
    }

    public int recordSize() {
        return data.remaining();
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

    public int writeToIndex(Index index, long recordPos) {
        int recSize = recordSize();
        index.write(data, Record2.KEY_OFFSET, keySize(), recSize, recordPos);
        return recSize;
    }

    int valueOffset() {
        int recSize = recordSize();
        int valSize = valueSize();
        return recSize - valSize;
    }

    public int create(long sequence, ByteBuffer key, ByteBuffer value, ByteBuffer dst, int... attr) {
        int checksum = ByteBufferChecksum.crc32(value);

        int recordStart = dst.position();

        int recLen = 0;
        dst.putInt(HEADER_BYTES + key.remaining() + value.remaining()); // RECORD_LEN (including this field)
        dst.putInt(value.remaining()); // VALUE_LEN
        dst.putLong(sequence); // SEQUENCE
        dst.putInt(checksum); // CHECKSUM
        dst.putLong(System.currentTimeMillis()); // TIMESTAMP
        dst.put(attribute(attr)); // ATTRIBUTES

        Buffers.copy(key, dst);
        Buffers.copy(value, dst);

        int recordEnd = dst.position();

        dst.position(recordStart);
        if (!Record.isValid(dst)) {
            throw new RuntimeException("Invalid record");
        }
        dst.position(recordEnd);

        return recLen;
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

    private byte attribute(int... attributes) {
        byte b = 0;
        for (int attr : attributes) {
            b = (byte) (b | 1 << attr);
        }
        return b;
    }

    public int writeKey(ByteBuffer dst) {
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
        owner.free(this);
    }
}
