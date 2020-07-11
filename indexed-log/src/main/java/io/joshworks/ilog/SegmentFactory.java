package io.joshworks.ilog;

import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.RecordPool;

import java.io.File;

@FunctionalInterface
public interface SegmentFactory<T extends IndexedSegment> {

    T create(File file, long indexEntries, RowKey rowKey, RecordPool pool);

}
