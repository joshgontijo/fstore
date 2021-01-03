package io.joshworks.es2;

import java.io.Closeable;

public interface SegmentFile extends Closeable, Comparable<SegmentFile> {

    @Override
    void close();

    void delete();

    String name();

    @Override
    default int compareTo(SegmentFile o) {
        return name().compareTo(o.name()) * -1;
    }
}
