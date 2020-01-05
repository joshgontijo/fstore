package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.ChecksumException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * DATA_LEN (4 BYTES)
 * CHECKSUM (4 BYTES)
 * TIMESTAMP (8 BYTES)
 * KEY_LEN (4 BYTES)
 * ATTR (4 BYTES)
 * <p>
 * [KEY] (N BYTES)
 * [VALUE] (N BYTES)
 */
public class Record {

    private static final int DATA_LEN_LEN = Integer.BYTES;
    private static final int CHECKSUM_LEN = Integer.BYTES;
    private static final int TIMESTAMP_LEN = Long.BYTES;
    private static final int KEY_LEN_LEN = Integer.BYTES;
    private static final int ATTR_LEN = Byte.BYTES;

    public static final int HEADER_BYTES = DATA_LEN_LEN + CHECKSUM_LEN + TIMESTAMP_LEN + KEY_LEN_LEN + ATTR_LEN;

    public static final int DATA_LENGTH_OFFSET = 0;
    public static final int CHECKSUM_OFFSET = DATA_LENGTH_OFFSET + DATA_LEN_LEN;
    public static final int TIMESTAMP_OFFSET = CHECKSUM_OFFSET + CHECKSUM_LEN;
    public static final int KEY_LENGTH_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LEN;
    public static final int ATTR_OFFSET = KEY_LENGTH_OFFSET + KEY_LEN_LEN;

    final ByteBuffer buffer; //package private for testing

    private Record(ByteBuffer buffer) {
        this.buffer = buffer;
        int recordLength = recordLength();
        if (recordLength != buffer.limit()) {
            throw new IllegalStateException("Unexpected buffer size " + buffer.limit() + " record length: " + recordLength);
        }
    }

    public void writeKey(MappedFile mf) {
        int keyStart = HEADER_BYTES;
        int keyLen = keyLength();

        var mbb = mf.buffer();
        Buffers.copy(buffer, keyStart, keyLen, mbb);

    }

    public int dataLength() {
        return buffer.getInt(DATA_LENGTH_OFFSET);
    }

    public int checksum() {
        return buffer.getInt(CHECKSUM_OFFSET);
    }

    public long timestamp() {
        return buffer.getLong(TIMESTAMP_OFFSET);
    }

    public int keyLength() {
        return buffer.getInt(KEY_LENGTH_OFFSET);
    }

    public int recordLength() {
        return HEADER_BYTES + keyLength() + dataLength();
    }

    public ByteBuffer data() {
        int valueStart = HEADER_BYTES + keyLength();
        return buffer.duplicate().position(valueStart);
    }

    public ByteBuffer key() {
        int keyStart = HEADER_BYTES;
        int keyLen = keyLength();
        return buffer.duplicate().limit(keyStart + keyLen).position(keyStart);
    }

    public boolean hasAttribute(int attribute) {
        byte attr = buffer.get(ATTR_OFFSET);
        return (attr & (Byte.MAX_VALUE << attribute)) == 1;
    }

    public static <K, V> Record create(K key, Serializer<K> ks, V value, Serializer<V> vs, ByteBuffer writeBuffer) {
        if (writeBuffer.remaining() <= HEADER_BYTES) {
            throw new IllegalArgumentException("Write buffer must be at least " + HEADER_BYTES);
        }
        try {
            int recordStart = writeBuffer.position();
            int originalLimit = writeBuffer.limit();
            writeBuffer.position(HEADER_BYTES);
            int keyStart = writeBuffer.position();
            ks.writeTo(key, writeBuffer);
            int keyEnd = writeBuffer.position();
            vs.writeTo(value, writeBuffer);
            int dataEnd = writeBuffer.position();

            int keyLen = keyEnd - keyStart;
            int dataLen = dataEnd - keyEnd;
            ByteBuffer dataSlice = writeBuffer.limit(dataEnd).position(keyEnd);
            int checksum = ByteBufferChecksum.crc32(dataSlice);
            writeBuffer.position(recordStart);

            writeBuffer.putInt(dataLen);
            writeBuffer.putInt(checksum);
            writeBuffer.putLong(System.currentTimeMillis());
            writeBuffer.putInt(keyLen);
            writeBuffer.put((byte) 0); //NO ATTRIBUTE TODO ?

            writeBuffer.position(recordStart).limit(dataEnd);

            int recordLen = writeBuffer.remaining();
            var recordBuffer = Buffers.allocate(recordLen, false);
            recordBuffer.put(writeBuffer);
            recordBuffer.flip();

            writeBuffer.limit(originalLimit);
            writeBuffer.position(dataEnd);

            return new Record(recordBuffer);

        } catch (Exception e) {
            throw new RuntimeException("Failed to create record", e);
        }
    }

    /**
     * Read from a slice of data, if slice has less data than the required to build a record then null is returned
     */
    public static Record from(ByteBuffer data, boolean copy) {
        try {
            int remaining = data.remaining();
            if (remaining < HEADER_BYTES) {
                return null;
            }

            int recordLength = Record.recordLength(data);
            if (recordLength > remaining) {
                return null;
            }

            Record record = readRecord(data, copy, recordLength);

            ByteBuffer d = record.data();
            int checksum = record.checksum();
            verifyChecksum(d, checksum);

            return record;

        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid record", e);
        }
    }

    private static Record readRecord(ByteBuffer data, boolean copy, int recordLength) {
        if (copy) {
            var copyBuffer = Buffers.allocate(recordLength, data.isDirect());
            int prevLimit = data.limit();
            data.limit(data.position() + recordLength);
            copyBuffer.put(data);
            data.limit(prevLimit);
            copyBuffer.flip();
            return new Record(copyBuffer);
        }

        int recordStart = data.position();
        int prevLimit = data.limit();
        ByteBuffer slice = data.limit(recordStart + recordLength).slice();
        data.limit(prevLimit);

        Buffers.offsetPosition(data, recordLength);
        return new Record(slice);
    }

    private static void verifyChecksum(ByteBuffer data, int checksum) {
        int computedChecksum = ByteBufferChecksum.crc32(data);
        if (computedChecksum != checksum) {
            throw new ChecksumException();
        }
    }

    static int recordLength(ByteBuffer buffer) {
        int relativePos = buffer.position();
        int recordLen = HEADER_BYTES + buffer.getInt(relativePos + KEY_LENGTH_OFFSET) + buffer.getInt(relativePos + DATA_LENGTH_OFFSET);
        if (recordLen <= HEADER_BYTES) {
            throw new IllegalStateException("Invalid record length found " + recordLen);
        }
        return recordLen;
    }

    public int appendTo(FileChannel channel) throws IOException {
        return channel.write(buffer);
    }

    @Override
    public String toString() {
        return "Record{" +
                " recordLength=" + recordLength() +
                ", keyLength=" + keyLength() +
                ", dataLength=" + dataLength() +
                ", timestamp=" + timestamp() +
                ", attributes=" + buffer.get(ATTR_OFFSET) +
                '}';
    }
}
