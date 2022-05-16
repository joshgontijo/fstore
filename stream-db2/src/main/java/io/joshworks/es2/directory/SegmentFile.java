package io.joshworks.es2.directory;

import java.io.Closeable;

public interface SegmentFile extends Closeable, Comparable<SegmentFile> {

    @Override
    void close();

    void delete();

    String name();

    long size();

    default SegmentId segmentId() {
        return SegmentId.from(name());
    }

    @Override
    default int compareTo(SegmentFile o) {
        var thisSegmentId = DirectoryUtils.segmentId(this);
        var otherSegmentId = DirectoryUtils.segmentId(o);
        return thisSegmentId.compareTo(otherSegmentId);
    }
}
