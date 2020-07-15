package io.joshworks.ilog;

import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.SSTable;
import io.joshworks.ilog.record.RecordPool;

import java.io.File;

@FunctionalInterface
public interface SegmentFactory<T extends Segment> {

    T create(File file, RecordPool pool, long maxSize, long maxEntries);

    static SegmentFactory<IndexedSegment> indexed(RowKey rowKey) {
        return (file, pool, maxSize, maxEntries) -> new IndexedSegment(file, pool, rowKey, maxEntries);
    }

    static SegmentFactory<SSTable> sstable(RowKey rowKey) {
        return (file, pool, maxSize, maxEntries) -> new SSTable(file, pool, rowKey, maxEntries);
    }


}
