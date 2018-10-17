package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.SegmentException;

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

    public static LogHeader getOrCreate(Storage storage, String magic, Type type) {
        LogHeader foundHeader = LogHeader.read(storage);
        if (foundHeader == null) {
            if (Type.OPEN.equals(type)) {
                IOUtils.closeQuietly(storage);
                throw new SegmentException("Segment doesn't exist, " + Type.LOG_HEAD + " or " + Type.MERGE_OUT + " must be specified");
            }
            foundHeader = LogHeader.create(magic, type);
            LogHeader.write(storage, foundHeader);
        }
        validateMagic(foundHeader.magic, magic);
        return foundHeader;
    }

    private static void validateMagic(String actualMagic, String expectedMagic) {
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
        return headerSerializer.fromBytes(bb);
    }

    public static void write(Storage storage, LogHeader header) {
        try {
            long pos = storage.position();
            storage.position(0);
            ByteBuffer headerData = headerSerializer.toBytes(header);
            if (storage.write(headerData) != LogHeader.BYTES) {
                throw new IllegalStateException("Unexpected written header length");
            }
            storage.position(pos);
            storage.flush();
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header");
        }
    }

    private static void validateTypeProvided(Type type) {
        //new segment, create a header
        if (type == null) {
            throw new IllegalArgumentException("Type must provided when creating a new segment");
        }
        if (!Type.LOG_HEAD.equals(type) && !Type.MERGE_OUT.equals(type)) {
            throw new IllegalArgumentException("Only LOG_HEAD and MERGE_OUT are accepted when creating a segment");
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
