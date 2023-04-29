package io.joshworks.es2.directory;

import io.joshworks.fstore.core.util.BitUtil;

import static java.lang.Long.compare;

public record SegmentId(int level, long idx) implements Comparable<SegmentId> {

    public static final String SEPARATOR = "-";

    public SegmentId {
        if (idx < 0 || level < 0) {
            throw new RuntimeException("Invalid segment values, level: " + level + ", idx: " + idx);
        }
    }

    @Override
    public int compareTo(SegmentId o) {
        var levelDiff = Integer.compare(level(), o.level());
        if (levelDiff != 0) {
            return levelDiff;
        }
        return compare(idx(), o.idx()) * -1; //reversed
    }

    @Override
    public String toString() {
        String fileLevel = toLevelString(level);
        String fileLevelIdx = String.format("%0" + BitUtil.decimalUnitsForDecimal(Long.MAX_VALUE) + "d", idx);
        return fileLevel + SEPARATOR + fileLevelIdx;
    }

    public static SegmentId of(int level, long idx) {
        return new SegmentId(level, idx);
    }

    public static SegmentId from(String id) {
        String[] part = id.split(SEPARATOR);
        int level = Integer.parseInt(part[0]);
        long idx = Long.parseLong(part[1]);
        return new SegmentId(level, idx);
    }

    private static String toLevelString(int level) {
        return String.format("%0" + BitUtil.decimalUnitsForDecimal(Integer.MAX_VALUE) + "d", level);
    }
}
