package io.joshworks.fstore.log.segment.header;

public final class CompletedSection {
    public final int level; //segments created are implicit level zero
    public final long entries;
    public final long writePosition; //actual written bytes, including header
    public final long rolled;
    public final long uncompressedSize;

    public CompletedSection(int level, long entries, long writePosition, long rolled, long uncompressedSize) {
        this.level = level;
        this.entries = entries;
        this.writePosition = writePosition;
        this.rolled = rolled;
        this.uncompressedSize = uncompressedSize;
    }

    @Override
    public String toString() {
        return "{" +
                "level=" + level +
                ", entries=" + entries +
                ", writePosition=" + writePosition +
                ", rolled=" + rolled +
                ", uncompressedSize=" + uncompressedSize +
                '}';
    }
}
