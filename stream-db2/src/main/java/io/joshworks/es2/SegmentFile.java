package io.joshworks.es2;

import java.io.Closeable;
import java.io.File;

public interface SegmentFile extends Closeable, Comparable<SegmentFile> {

    @Override
    void close();

    void delete();

    File file();

    @Override
    default int compareTo(SegmentFile o) {
        return file().getName().compareTo(o.file().getName());
    }
}
