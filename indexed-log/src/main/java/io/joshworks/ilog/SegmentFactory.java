package io.joshworks.ilog;

import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.SSTable;
import io.joshworks.ilog.record.RecordPool;

import java.io.File;

@FunctionalInterface
public interface SegmentFactory<T extends Segment> {

    T create(File file, RecordPool pool, long maxSize);

    static SegmentFactory<IndexedSegment> indexed(RowKey rowKey, long indexEntries) {
        return (file, pool, maxSize) -> new IndexedSegment(file, pool, rowKey, indexEntries);
    }

    static SegmentFactory<SSTable> sstable(RowKey rowKey, long indexEntries) {
        return (file, pool, maxSize) -> new SSTable(file, pool, rowKey, indexEntries);
    }


}
