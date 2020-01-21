package io.joshworks.ilog;

import io.joshworks.ilog.index.Index;

import java.io.File;

@FunctionalInterface
public interface SegmentFactory<T extends IndexedSegment> {

    T create(File file, Index index);

}
