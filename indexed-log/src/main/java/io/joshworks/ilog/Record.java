package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.log.record.ByteBufferChecksum;
import io.joshworks.fstore.log.record.ChecksumException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.joshworks.ilog.RecordHeader.HEADER_BYTES;

public class Record {

    public final int length;
    public final long offset;
    public final long timestamp;
    public final int checksum;
    public final ByteBuffer data;

    private Record(long offset, int checksum, int length, long timestamp, ByteBuffer data) {
        this.offset = offset;
        this.checksum = checksum;
        this.length = length;
        this.timestamp = timestamp;
        this.data = data;
    }

    public static Record create(ByteBuffer data, long offset) {
        int checksum = ByteBufferChecksum.crc32(data);
        int length = data.remaining();
        long timestamp = System.currentTimeMillis();
        return new Record(offset, checksum, length, timestamp, data);
    }

    public static Record from(ByteBuffer data, boolean copyBuffer) {
        int length = data.getInt();
        int checksum = data.getInt();
        long offset = data.getLong();
        long timestamp = data.getLong();

        if (length > data.remaining()) {
            throw new IllegalStateException("Invalid entry");
        }

        ByteBuffer copy;
        int limit = data.limit();
        if (copyBuffer) {
            data.limit(data.position() + length);
            copy = Buffers.allocate(length, data.isDirect());
            copy.put(data);
            copy.flip();
        } else {
            data.limit(data.position() + length);
            copy = data.slice().asReadOnlyBuffer();
        }
        data.position(data.limit());
        data.limit(limit);
        verifyChecksum(copy, checksum);
        return new Record(offset, checksum, length, timestamp, copy);
    }

    private static void verifyChecksum(ByteBuffer data, int checksum) {
        int computedChecksum = ByteBufferChecksum.crc32(data);
        if (computedChecksum != checksum) {
            throw new ChecksumException();
        }
    }

    public int appendTo(FileChannel channel, ByteBuffer writeBuffer) {
        try {
            writeBuffer.putInt(length);
            writeBuffer.putInt(checksum);
            writeBuffer.putLong(offset);
            writeBuffer.putLong(timestamp);
            writeBuffer.put(data);

            writeBuffer.flip();
            return channel.write(writeBuffer);

        } catch (IOException e) {
            throw new RuntimeIOException("Failed to write record offset " + offset, e);
        }
    }

    /**
     * Position is the start of the record (before the header)
     */
    public static Record readFrom(FileChannel channel, RecordHeader header, long position) {
        try {
            ByteBuffer data = Buffers.allocate(header.length, false);
            int read = channel.read(data, position + HEADER_BYTES);
            if (read != header.length) {
                throw new RuntimeIOException("Invalid record data, expected " + header.length + ", got " + read);
            }
            data.flip();
            verifyChecksum(data, header.checksum);
            return new Record(header.offset, header.checksum, header.length, header.timestamp, data);

        } catch (IOException e) {
            throw new RuntimeIOException("Failed to read record at position " + position, e);
        }
    }

    public long size() {
        return HEADER_BYTES + length;
    }

    public int dataSize() {
        return data.capacity();
    }

    @Override
    public String toString() {
        return "Record{" +
                "offset=" + offset +
                ", length=" + length +
                ", timestamp=" + timestamp +
                ", checksum=" + checksum +
                '}';
    }
}
