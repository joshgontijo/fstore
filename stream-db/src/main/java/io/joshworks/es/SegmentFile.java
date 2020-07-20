package io.joshworks.es;

import java.io.Closeable;
import java.io.File;

public interface SegmentFile extends Closeable {

    @Override
    void close();

    void delete();

    File file();
}
