package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.log.record.ByteBufferChecksum;
import io.joshworks.fstore.log.record.ChecksumException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Record {

    private final int length;
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

    public static Record create(ByteBuffer data, int offset) {
        int checksum = ByteBufferChecksum.crc32(data);
        int length = data.remaining();
        long timestamp = System.currentTimeMillis();
        return new Record(offset, checksum, length, timestamp, data);
    }

    public static Record from(ByteBuffer data) {
        int length = data.getInt();
        int checksum = data.getInt();
        long offset = data.getLong();
        long timestamp = data.getLong();

        if (length > data.remaining() - Integer.BYTES) {
            throw new IllegalStateException("Invalid entry");
        }

        data.limit(data.position() + length);
        ByteBuffer copy = Buffers.allocate(length, data.isDirect());
        copy.put(data);
        copy.flip();

        int computedChecksum = ByteBufferChecksum.crc32(copy);
        if (computedChecksum != checksum) {
            throw new ChecksumException();
        }

        return new Record(offset, checksum, length, timestamp, copy);
    }

    public int appendTo(FileChannel channel, ByteBuffer writeBuffer) {
        try {
            writeBuffer.putInt(length);
            writeBuffer.putInt(checksum);
            writeBuffer.putLong(offset);
            writeBuffer.putLong(timestamp);
            writeBuffer.put(data);
            writeBuffer.putInt(length);

            writeBuffer.flip();
            return channel.write(writeBuffer);

        } catch (IOException e) {
            throw new RuntimeIOException("Failed to write record offset " + offset, e);
        }
    }

    public long recordSize() {
        return length + (Integer.BYTES * 3) + (Long.BYTES * 2);
    }
}
