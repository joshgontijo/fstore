package io.joshworks.ilog.lsm;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.polled.ObjectPool;
import io.joshworks.ilog.record.HeapBlock;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.nio.ByteBuffer;

public class SSTable extends IndexedSegment {

    private final ObjectPool<HeapBlock> blockPool;

    public SSTable(File file, long indexEntries, RecordPool pool, ObjectPool<HeapBlock> blockPool) {
        super(file, indexEntries, pool);
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
            return block.find(key, IndexFunction.EQUALS);
        }
    }

    public HeapBlock readBlock(ByteBuffer key, IndexFunction func) {
        try (Records records = pool.read(this, key, func)) {
            Record2 blockRec = records.poll();
            if (blockRec == null) {
                return null;
            }
            HeapBlock block = blockPool.allocate();
            block.from(blockRec, true);
            return block;
        }
    }

}
