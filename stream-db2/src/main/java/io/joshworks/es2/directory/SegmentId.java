package io.joshworks.es2.directory;

public record SegmentId(int level, long idx) implements Comparable<SegmentId>{

    @Override
    public int compareTo(SegmentId o) {
        var levelDiff = Integer.compare(level(), o.level());
        if (levelDiff != 0) {
            return levelDiff;
        }
        return Long.compare(idx(), o.idx()) * -1; //reversed
    }
}
