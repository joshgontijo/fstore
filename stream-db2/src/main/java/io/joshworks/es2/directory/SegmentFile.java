package io.joshworks.es2.directory;

import java.io.Closeable;

public interface SegmentFile extends Closeable, Comparable<SegmentFile> {

    @Override
    void close();

    void delete();

    String name();

    long size();

    @Override
    default int compareTo(SegmentFile o) {
        var thisSegmentId = DirectoryUtils.segmentId(this);
        var otherSegmentId = DirectoryUtils.segmentId(o);

        var levelDiff = Integer.compare(thisSegmentId.level(), otherSegmentId.level());
        if (levelDiff != 0) {
            return levelDiff;
        }
        return Long.compare(thisSegmentId.idx(), otherSegmentId.idx()) * -1; //reversed
    }
}
