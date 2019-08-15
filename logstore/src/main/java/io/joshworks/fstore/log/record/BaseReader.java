package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

public class BaseReader {

    protected final ThreadLocalBufferPool bufferPool;
    final int pageReadSize;
    private final ThreadLocalRandom rand = ThreadLocalRandom.current();
    private final double checksumProb;

    public BaseReader(ThreadLocalBufferPool bufferPool, double checksumProb, int pageReadSize) {
        this.bufferPool = bufferPool;
        this.checksumProb = checksumProb;
        this.pageReadSize = pageReadSize;
    }

    protected void checksum(int expected, ByteBuffer data, long position) {
        if (checksumProb == 0) {
            return;
        }
        if (checksumProb >= 100 && ByteBufferChecksum.crc32(data) != expected) {
            throw new ChecksumException(position);
        }
        if (rand.nextInt(100) < checksumProb && ByteBufferChecksum.crc32(data) != expected) {
            throw new ChecksumException(position);
        }
    }
}
