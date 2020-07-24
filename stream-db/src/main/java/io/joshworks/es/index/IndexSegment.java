package io.joshworks.es.index;

import io.joshworks.es.SegmentFile;

public interface IndexSegment extends SegmentFile {
    void append(long stream, int version, long logPos);

    IndexEntry find(IndexKey key, IndexFunction func);

    boolean isFull();

    int entries();

    void truncate();

    void complete();

    String name();

    int size();
}
