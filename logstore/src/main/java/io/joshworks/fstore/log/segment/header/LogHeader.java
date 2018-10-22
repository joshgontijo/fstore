package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public class LogHeader {

    private static final Serializer<LogHeader> headerSerializer = new HeaderSerializer();
    public static final int BYTES = 1024;

    //segment info
    public final String magic;
    public final int level;
    public final long created;
    public final Type type;
    public final long segmentSize;

    //log info
    public final long logStart;
    public final long logEnd;
    public final long entries;

    //footer info
    public final long footerStart;
    public final long footerEnd;

    private LogHeader(String magic, long entries, long created, int level, Type type, long segmentSize, long logStart, long logEnd, long footerStart, long footerEnd) {
        this.magic = magic;
        this.entries = entries;
        this.created = created;
        this.level = level;
        this.type = type;
        this.segmentSize = segmentSize;
        this.logStart = logStart;
        this.logEnd = logEnd;
        this.footerStart = footerStart;
        this.footerEnd = footerEnd;
    }

    public static LogHeader create(String magic, Type type) {
        return new LogHeader(magic, 0, System.currentTimeMillis(), 0, type, 0, 0, 0, 0, 0);
    }

    public static LogHeader create(String magic, long entries, long created, int level, Type type, long segmentSize, long logStart, long logEnd, long footerStart, long footerEnd) {
        return new LogHeader(magic, entries, created, level, type, segmentSize, logStart, logEnd, footerStart, footerEnd);
    }

    public static void validateMagic(String actualMagic, String expectedMagic) {
        byte[] actual = actualMagic.getBytes(StandardCharsets.UTF_8);
        byte[] expected = expectedMagic.getBytes(StandardCharsets.UTF_8);
        if (!Arrays.equals(expected, actual)) {
            throw new InvalidMagic(expectedMagic, actualMagic);
        }
    }

    public static LogHeader read(Storage storage) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.BYTES);
        storage.read(0, bb);
        bb.flip();
        if (bb.remaining() == 0) {
            return null;
        }
        int length = bb.getInt();
        if (length == 0) {
            return null;
        }
        int checksum = bb.getInt();
        bb.limit(bb.position() + length); //length + checksum
        if (Checksum.crc32(bb) != checksum) {
            throw new IllegalStateException("Log head checksum verification failed");
        }

        return headerSerializer.fromBytes(bb);
    }

    public static LogHeader write(Storage storage, LogHeader header) {
        try {
            ByteBuffer withChecksumAndLength = ByteBuffer.allocate(BYTES);
            ByteBuffer headerData = headerSerializer.toBytes(header);

            int entrySize = headerData.remaining();
            withChecksumAndLength.putInt(entrySize);
            withChecksumAndLength.putInt(Checksum.crc32(headerData));
            withChecksumAndLength.put(headerData);
            withChecksumAndLength.position(0);//do not flip, the header will always have the fixed size

            long prevPos = storage.position();
            storage.position(0);
            if (storage.write(withChecksumAndLength) != LogHeader.BYTES) {
                throw new IllegalStateException("Unexpected written header length");
            }
            storage.position(prevPos);
            return header;
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogHeader header = (LogHeader) o;
        return created == header.created &&
                level == header.level &&
                segmentSize == header.segmentSize &&
                logStart == header.logStart &&
                logEnd == header.logEnd &&
                entries == header.entries &&
                footerStart == header.footerStart &&
                footerEnd == header.footerEnd &&
                Objects.equals(magic, header.magic) &&
                type == header.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(magic, created, level, type, segmentSize, logStart, logEnd, entries, footerStart, footerEnd);
    }

    @Override
    public String toString() {
        return "Header{" + "magic='" + magic + '\'' +
                ", created=" + created +
                ", level=" + level +
                ", type=" + type +
                ", segmentSize=" + segmentSize +
                ", logStart=" + logStart +
                ", size=" + logEnd +
                ", entries=" + entries +
                ", footerPos=" + footerStart +
                ", footerEnd=" + footerEnd +
                '}';
    }
}
