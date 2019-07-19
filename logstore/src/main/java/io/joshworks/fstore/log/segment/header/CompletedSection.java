package io.joshworks.fstore.log.segment.header;

import java.util.Objects;

public final class CompletedSection {
    public final int level; //segments created are implicit level zero
    public final long entries;
    public final long rolled;
    public final long uncompressedSize;
    public final long actualDataLength;
    public final long footerMapPosition;
    public final long footerLength;
    public final long physical;

    public CompletedSection(int level, long entries, long actualDataLength, long footerMapPosition, long footerLength, long rolled, long uncompressedSize, long physical) {
        this.level = level;
        this.entries = entries;
        this.rolled = rolled;
        this.uncompressedSize = uncompressedSize;
        this.actualDataLength = actualDataLength;
        this.footerMapPosition = footerMapPosition;
        this.footerLength = footerLength;
        this.physical = physical;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompletedSection that = (CompletedSection) o;
        return level == that.level &&
                entries == that.entries &&
                rolled == that.rolled &&
                uncompressedSize == that.uncompressedSize &&
                actualDataLength == that.actualDataLength &&
                footerMapPosition == that.footerMapPosition &&
                footerLength == that.footerLength &&
                physical == that.physical;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, entries, rolled, uncompressedSize, actualDataLength, footerMapPosition, footerLength, physical);
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
                ", physical=" + physical +
                '}';
    }
}
