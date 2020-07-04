package io.joshworks.ilog;

import io.joshworks.ilog.record.RecordPool;

import java.io.File;

@FunctionalInterface
public interface SegmentFactory<T extends IndexedSegment> {

    T create(File file, int indexSize, RecordPool pool);

}
