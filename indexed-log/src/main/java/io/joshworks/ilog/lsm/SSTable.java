package io.joshworks.ilog.lsm;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.polled.ObjectPool;
import io.joshworks.ilog.record.HeapBlock;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.nio.ByteBuffer;

public class SSTable extends IndexedSegment {

    private final ObjectPool<HeapBlock> blockPool;

    public SSTable(File file, long indexEntries, RowKey rowKey, RecordPool pool, ObjectPool<HeapBlock> blockPool) {
        super(file, indexEntries, rowKey, pool);
        this.blockPool = blockPool;
    }

    public Records find(ByteBuffer key, IndexFunction func) {
        if (!readOnly()) {
            return null;
        }

        try (HeapBlock block = readBlock(key, func)) {
            if (block == null) {
                return null;
            }
            int idx = block.indexOf(key, IndexFunction.EQUALS);
            if (idx == Index.NONE) {
                return null;
            }
            return block.read(idx);
        }
    }

    public HeapBlock readBlock(ByteBuffer key, IndexFunction func) {
        try (Records records = super.get(key, func)) {
            if (records.isEmpty()) {
                return null;
            }
            Record blockRec = records.get(0);
            HeapBlock block = blockPool.allocate();
            block.from(blockRec);
            return block;
        }
    }

}
