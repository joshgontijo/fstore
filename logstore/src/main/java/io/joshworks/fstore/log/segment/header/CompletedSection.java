package io.joshworks.fstore.log.segment.header;

public final class CompletedSection {
    public final int level; //segments created are implicit level zero
    public final long entries;
    public final long rolled;
    public final long uncompressedSize;
    public final long actualDataSize;
    public final long footerLength;

    public CompletedSection(int level, long entries, long actualDataSize, long footerLength, long rolled, long uncompressedSize) {
        this.level = level;
        this.entries = entries;
        this.rolled = rolled;
        this.uncompressedSize = uncompressedSize;
        this.actualDataSize = actualDataSize;
        this.footerLength = footerLength;
    }

    @Override
    public String toString() {
        return "{" +
                "level=" + level +
                ", entries=" + entries +
                ", actualDataSize=" + actualDataSize +
                ", footerLength=" + footerLength +
                ", rolled=" + rolled +
                ", uncompressedSize=" + uncompressedSize +
                '}';
    }
}
