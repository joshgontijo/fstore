package io.joshworks.fstore.core.util;

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
        return checksum(buffer, new CRC32());
    }

    public static int crc32c(ByteBuffer buffer) {
        return checksum(buffer, new CRC32C());
    }

    public static int adler32(ByteBuffer buffer) {
        return checksum(buffer, new Adler32());
    }

    private static int checksum(ByteBuffer buffer, Checksum impl) {
        if (!buffer.hasArray()) {
            byte[] data = new byte[buffer.remaining()];
            buffer.mark();
            buffer.get(data);
            buffer.reset();
            return checksum(impl, data, 0, data.length);
        }
        int offset = buffer.arrayOffset();
        return checksum(impl, buffer.array(), offset + buffer.position(), buffer.remaining());
    }

    private static int checksum(Checksum impl, byte[] data, int offset, int length) {
        impl.update(SEED);
        impl.update(data, offset, length);
        return (int) impl.getValue();
    }

}
