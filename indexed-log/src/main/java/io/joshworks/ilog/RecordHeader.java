package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class RecordHeader {

    public static final int HEADER_BYTES = (Integer.BYTES * 2) + (Long.BYTES * 2);

    public final int length;
    public final long offset;
    public final long timestamp;
    public final int checksum;

    private RecordHeader(int length, long offset, long timestamp, int checksum) {
        this.length = length;
        this.offset = offset;
        this.timestamp = timestamp;
        this.checksum = checksum;
    }

    public static RecordHeader readFrom(FileChannel channel, ByteBuffer buffer, long position) {
        try {
            int read = channel.read(buffer, position);
            buffer.flip();
            if (read != HEADER_BYTES) {
                throw new RuntimeIOException("Invalid header length, expected " + HEADER_BYTES + ", got " + read);
            }
            int length = buffer.getInt();
            int checksum = buffer.getInt();
            long offset = buffer.getLong();
            long timestamp = buffer.getLong();

            return new RecordHeader(length, offset, timestamp, checksum);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to read record header", e);
        }

    }

    public static RecordHeader parse(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_BYTES) {
            throw new RuntimeIOException("Invalid header length, expected " + HEADER_BYTES + ", got " + buffer.remaining());
        }
        int length = buffer.getInt();
        int checksum = buffer.getInt();
        long offset = buffer.getLong();
        long timestamp = buffer.getLong();

        return new RecordHeader(length, offset, timestamp, checksum);
    }

}