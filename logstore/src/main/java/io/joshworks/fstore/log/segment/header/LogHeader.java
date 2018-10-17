package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.SegmentException;

import java.io.File;
import java.nio.ByteBuffer;
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

    public static LogHeader noHeader() {
        return new LogHeader("NO-MAGIC", 0, -1, -1, Type.EMPTY, -1, -1, -1, -1, -1);
    }

    public static LogHeader create(String magic, Type type) {
        return new LogHeader(magic, 0, System.currentTimeMillis(), 0, type, 0, 0, 0, 0, 0);
    }

    public static LogHeader create(String magic, long entries, long created, int level, Type type, long segmentSize, long logStart, long logEnd, long footerStart, long footerEnd) {
        return new LogHeader(magic, entries, created, level, type, segmentSize, logStart, logEnd, footerStart, footerEnd);
    }


    public static LogHeader getOrCreate(File file, String magic, Type type) {
        LogHeader read = read(file);
        return read != null ? read : write(file, magic, type);
    }

    public static LogHeader read(File file) {
        try(Storage storage = new RafStorage(file, 0, Mode.READ)) {

            ByteBuffer bb = ByteBuffer.allocate(LogHeader.BYTES);
            storage.read(0, bb);
            bb.flip();
            if (bb.remaining() == 0) {
                return null;
            }
            return headerSerializer.fromBytes(bb);

        }catch (Exception e) {
            throw new RuntimeException("Failed to read header");
        }

    }

    public static LogHeader write(File file, String magic, Type type) {
        try(Storage storage = new RafStorage(file, LogHeader.BYTES, Mode.READ)) {
            validateTypeProvided(type);
            LogHeader newHeader = LogHeader.create(magic, type);
            ByteBuffer headerData = headerSerializer.toBytes(newHeader);
            if (storage.write(headerData) != LogHeader.BYTES) {
                throw new SegmentException("Failed to create header");
            }
            storage.flush();
            return newHeader;

        }catch (Exception e) {
            throw new RuntimeException("Failed to read header");
        }

    }

    private static void validateTypeProvided(Type type) {
        //new segment, create a header
        if (type == null) {
            throw new IllegalArgumentException("Type must provided when creating a new segment");
        }
        if (!Type.LOG_HEAD.equals(type) && !Type.MERGE_OUT.equals(type)) {
            throw new IllegalArgumentException("Only Type.LOG_HEAD and Type.MERGE_OUT are accepted when creating a segment");
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
