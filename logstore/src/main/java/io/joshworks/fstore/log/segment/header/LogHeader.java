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

    //newHeader
    public final String magic;
    public final long created;
    public final Type type;
    public final long logEnd;
    public final long fileSize;

    //completed info
    public final int level; //segments created are implicit level zero
    public final long entries;
    public final long logicalSize; //actual written bytes, including header
    public final long rolled;

    private LogHeader(String magic, long entries, long created, int level, Type type, long logEnd, long rolled, long fileSize, long logicalSize) {
        this.magic = magic;
        this.entries = entries;
        this.created = created;
        this.level = level;
        this.type = type;
        this.logEnd = logEnd;
        this.rolled = rolled;
        this.fileSize = fileSize;
        this.logicalSize = logicalSize;
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

    public static LogHeader writeNew(Storage storage, String magic, Type type, long logEnd, long fileSize) {
        LogHeader newHeader = new LogHeader(magic, 0, System.currentTimeMillis(), 0, type, logEnd, 0, fileSize, BYTES);
        write(storage, newHeader);
        return newHeader;
    }

    public static LogHeader writeCompleted(Storage storage, LogHeader initialHeader, long entries, int level, long logicalSize) {
        LogHeader newHeader = new LogHeader(
                initialHeader.magic,
                entries,
                initialHeader.created,
                level,
                Type.READ_ONLY,
                initialHeader.logEnd,
                System.currentTimeMillis(),
                initialHeader.fileSize,
                logicalSize);
        write(storage, newHeader);
        return newHeader;

    }

    private static LogHeader write(Storage storage, LogHeader header) {
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
        LogHeader logHeader = (LogHeader) o;
        return level == logHeader.level &&
                created == logHeader.created &&
                segmentSize == logHeader.segmentSize &&
                logStart == logHeader.logStart &&
                logEnd == logHeader.logEnd &&
                entries == logHeader.entries &&
                Objects.equals(magic, logHeader.magic) &&
                type == logHeader.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(magic, level, created, type, segmentSize, logStart, logEnd, entries);
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
                '}';
    }
}
