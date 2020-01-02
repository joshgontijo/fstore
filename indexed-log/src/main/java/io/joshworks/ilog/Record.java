package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.ChecksumException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.joshworks.ilog.RecordHeader.HEADER_BYTES;

public class Record {

    public final int dataSize;
    public final long offset;
    public final long timestamp;
    public final int checksum;
    public final ByteBuffer data;

    private Record(long offset, int checksum, int dataSize, long timestamp, ByteBuffer data) {
        this.offset = offset;
        this.checksum = checksum;
        this.dataSize = dataSize;
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
        RecordHeader header = RecordHeader.parse(data);
        return from(data, header, copyBuffer);
    }

    public static Record from(ByteBuffer data, RecordHeader header, boolean copyBuffer) {
        if (header.length > data.remaining()) {
            throw new RuntimeIOException("Failed to read record");
        }

        ByteBuffer copy;
        int limit = data.limit();
        if (copyBuffer) {
            data.limit(data.position() + header.length);
            copy = Buffers.allocate(header.length, data.isDirect());
            copy.put(data);
            copy.flip();
        } else {
            data.limit(data.position() + header.length);
            copy = data.slice().asReadOnlyBuffer();
        }
        data.position(data.limit());
        data.limit(limit);
        verifyChecksum(copy, header.checksum);
        return new Record(header.offset, header.checksum, header.length, header.timestamp, copy);
    }

    public static Record from(FileChannel channel, long position, int bufferSize) {
        if (bufferSize <= HEADER_BYTES) {
            throw new RuntimeException("bufferSize must be greater than " + HEADER_BYTES);
        }
        try {
            ByteBuffer buffer = Buffers.allocate(bufferSize, false);
            channel.read(buffer, position);
            buffer.flip();
            RecordHeader header = RecordHeader.parse(buffer);
            if (header.length > buffer.remaining()) {
                //too big, re read with a bigger buffer
                return from(channel, position, HEADER_BYTES + header.length);
            }
            return from(buffer, header, false);

        } catch (IOException e) {
            throw new RuntimeIOException("Failed to read record at position " + position, e);
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

    private static void verifyChecksum(ByteBuffer data, int checksum) {
        int computedChecksum = ByteBufferChecksum.crc32(data);
        if (computedChecksum != checksum) {
            throw new ChecksumException();
        }
    }

    public int appendTo(FileChannel channel, ByteBuffer writeBuffer) {
        try {
            writeBuffer.putInt(dataSize);
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

    public long size() {
        return HEADER_BYTES + dataSize;
    }

    public int dataSize() {
        return dataSize;
    }

    @Override
    public String toString() {
        return "Record{" +
                "offset=" + offset +
                ", dataSize=" + dataSize +
                ", size=" + size() +
                ", timestamp=" + timestamp +
                ", checksum=" + checksum +
                '}';
    }
}
