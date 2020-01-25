package io.joshworks.fstore.core.util;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public class ByteBufferChecksum {

    private static final byte[] SEED = ByteBuffer.allocate(4).putInt(456765723).array();

    private static ThreadLocal<Checksums> localCache = ThreadLocal.withInitial(Checksums::new);

    private ByteBufferChecksum() {
    }

    public static int crc32(ByteBuffer buffer) {
        return checksum(buffer, buffer.position(), buffer.remaining(), resetAndGet(localCache.get().crc32));
    }

    public static int crc32(ByteBuffer buffer, int pos, int len) {
        return checksum(buffer, pos, len, resetAndGet(localCache.get().crc32));
    }

    public static int crc32c(ByteBuffer buffer) {
        return checksum(buffer, buffer.position(), buffer.remaining(), resetAndGet(localCache.get().crc32c));
    }

    public static int crc32c(ByteBuffer buffer, int pos, int len) {
        return checksum(buffer, pos, len, resetAndGet(localCache.get().crc32c));
    }

    public static int adler32(ByteBuffer buffer) {
        return checksum(buffer, buffer.position(), buffer.remaining(), resetAndGet(localCache.get().adler32));
    }

    public static int adler32(ByteBuffer buffer, int pos, int len) {
        return checksum(buffer, pos, len, resetAndGet(localCache.get().adler32));
    }

    private static Checksum resetAndGet(Checksum checksum) {
        checksum.reset();
        return checksum;
    }

    private static int checksum(ByteBuffer buffer, int pos, int len, Checksum impl) {
        if (!buffer.hasArray()) {
            byte[] data = new byte[len];
            int ppos = buffer.position();
            buffer.get(data);
            buffer.position(ppos);
            return checksum(impl, data, pos, len);
        }
        pos = Buffers.absoluteArrayPosition(buffer, pos);
        return checksum(impl, buffer.array(), pos, len);
    }

    private static int checksum(Checksum impl, byte[] data, int offset, int length) {
        impl.update(SEED);
        impl.update(data, offset, length);
        return (int) impl.getValue();
    }

    private static class Checksums {
        private final Checksum crc32 = new CRC32();
        private final Checksum crc32c = new CRC32C();
        private final Checksum adler32 = new Adler32();
    }


}
