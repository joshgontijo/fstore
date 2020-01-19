package io.joshworks.fstore.core.util;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public class ByteBufferChecksum {

    private static final byte[] SEED = ByteBuffer.allocate(4).putInt(456765723).array();

    private ByteBufferChecksum() {
    }

    public static int crc32(ByteBuffer buffer) {
        return checksum(buffer, Buffers.absoluteArrayPosition(buffer), buffer.remaining(), new CRC32());
    }

    public static int crc32(ByteBuffer buffer, int pos, int len) {
        return checksum(buffer, pos, len, new CRC32());
    }

    public static int crc32c(ByteBuffer buffer) {
        return checksum(buffer, Buffers.absoluteArrayPosition(buffer), buffer.remaining(), new CRC32C());
    }

    public static int crc32c(ByteBuffer buffer, int pos, int len) {
        return checksum(buffer, pos, len, new CRC32C());
    }

    public static int adler32(ByteBuffer buffer) {
        return checksum(buffer, Buffers.absoluteArrayPosition(buffer), buffer.remaining(), new Adler32());
    }

    public static int adler32(ByteBuffer buffer, int pos, int len) {
        return checksum(buffer, pos, len, new CRC32C());
    }

    private static int checksum(ByteBuffer buffer, int pos, int len, Checksum impl) {
        if (!buffer.hasArray()) {
            byte[] data = new byte[len];
            int ppos = buffer.position();
            buffer.get(data);
            buffer.position(ppos);
            return checksum(impl, data, pos, len);
        }
        return checksum(impl, buffer.array(), pos, len);
    }

    private static int checksum(Checksum impl, byte[] data, int offset, int length) {
        impl.update(SEED);
        impl.update(data, offset, length);
        return (int) impl.getValue();
    }

}
