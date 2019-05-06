package io.joshworks.fstore.log.record;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

public class BaseReader {

    private final ThreadLocalRandom rand = ThreadLocalRandom.current();
    private final double checksumProb;
    private final int maxEntrySize;
    protected final int bufferSize;

    public BaseReader(double checksumProb, int maxEntrySize, int bufferSize) {
        this.checksumProb = checksumProb;
        this.maxEntrySize = maxEntrySize;
        this.bufferSize = bufferSize;
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

    protected void checkRecordLength(int length, long position) {
        if (length < 0 || length > maxEntrySize) {
            throw new IllegalStateException("Invalid record length " + length + " at position " + position);
        }
    }

}
