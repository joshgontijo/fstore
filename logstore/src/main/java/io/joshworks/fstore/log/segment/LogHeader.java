package io.joshworks.fstore.log.segment;

import java.util.Objects;

public class LogHeader {

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
                ", footerSize=" + footerEnd +
                '}';
    }
}
