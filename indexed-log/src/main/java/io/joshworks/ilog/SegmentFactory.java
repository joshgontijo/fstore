package io.joshworks.ilog;

import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.KeyComparator;

import java.io.File;

@FunctionalInterface
public interface SegmentFactory<T extends IndexedSegment> {

    T create(File file, int indexSize);

}
