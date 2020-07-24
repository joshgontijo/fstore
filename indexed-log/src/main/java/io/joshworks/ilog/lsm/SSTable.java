package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.RowKey;

import java.io.File;

public class SSTable extends IndexedSegment {

    public SSTable(File file, BufferPool pool, RowKey rowKey, long indexEntries) {
        super(file, pool, rowKey, indexEntries);
    }
}
