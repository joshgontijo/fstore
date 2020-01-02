package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Storage;

import java.nio.ByteBuffer;

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

    public static RecordHeader readFrom(Storage storage, ByteBuffer buffer, long position) {
        int read = storage.read(position, buffer);
        buffer.flip();
        if (read != HEADER_BYTES) {
            throw new RuntimeIOException("Invalid header length, expected " + HEADER_BYTES + ", got " + read);
        }
        int length = buffer.getInt();
        int checksum = buffer.getInt();
        long offset = buffer.getLong();
        long timestamp = buffer.getLong();

        return new RecordHeader(length, offset, timestamp, checksum);
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