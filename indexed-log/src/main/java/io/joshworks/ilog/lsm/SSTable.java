package io.joshworks.ilog.lsm;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.RecordPool;

import java.io.File;

public class SSTable extends IndexedSegment {

    public SSTable(File file, RecordPool pool, RowKey rowKey, long indexEntries) {
        super(file, pool, rowKey, indexEntries);
    }

}
