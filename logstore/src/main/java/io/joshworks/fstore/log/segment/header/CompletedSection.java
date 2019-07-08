package io.joshworks.fstore.log.segment.header;

public final class CompletedSection {
    public final int level; //segments created are implicit level zero
    public final long entries;
    public final long rolled;
    public final long uncompressedSize;
    public final long actualDataLength;
    public final long footerMapPosition;
    public final long footerLength;

    public CompletedSection(int level, long entries, long actualDataLength, long footerMapPosition, long footerLength, long rolled, long uncompressedSize) {
        this.level = level;
        this.entries = entries;
        this.rolled = rolled;
        this.uncompressedSize = uncompressedSize;
        this.actualDataLength = actualDataLength;
        this.footerMapPosition = footerMapPosition;
        this.footerLength = footerLength;
    }

    @Override
    public String toString() {
        return "CompletedSection{" + "level=" + level +
                ", entries=" + entries +
                ", rolled=" + rolled +
                ", uncompressedSize=" + uncompressedSize +
                ", actualDataLength=" + actualDataLength +
                ", footerMapPosition=" + footerMapPosition +
                ", footerLength=" + footerLength +
                '}';
    }
}
