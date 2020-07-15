package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.util.ObjectPool;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.record.Block;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.nio.ByteBuffer;

class SparseLsm extends Lsm {

    private final ObjectPool<Block> blockPool;

    SparseLsm(File root,
              RecordPool pool,
              RowKey rowKey,
              int memTableMaxEntries,
              long maxAge,
              int compactionThreshold,
              int blockSize,
              Codec codec) {
        super(root, pool, rowKey, memTableMaxEntries, maxAge, compactionThreshold);
        this.blockPool = new ObjectPool<>(p -> new Block(pool, blockSize, rowKey, codec));
    }

    @Override
    protected Record searchSSTable(ByteBuffer key, SSTable ssTable, IndexFunction func) {
        try (Block block = readBlock(ssTable, key)) {
            if (block == null) {
                return null;
            }
            int idx = block.indexOf(key, func);
            if (idx == Index.NONE) {
                return null;
            }
            return block.read(idx);
        }
    }

    public Block readBlock(SSTable ssTable, ByteBuffer key) {
        try (Record blockRec = ssTable.find(key, IndexFunction.FLOOR)) {
            if (blockRec == null) {
                return null;
            }
            Block block = blockPool.allocate();
            block.from(blockRec);
            return block;
        }
    }

    protected long flushMemTable() {
        long inserted = 0;

        Records records = pool.empty();

        try (Block block = blockPool.allocate()) {
            for (Node node : memTable) {
                boolean added = block.add(node.record());
                if (!added) {
                    if (records.isFull()) {
                        flushRecords(records);
                    }

                    inserted += block.entryCount();
                    block.write(records);
                    block.clear();


                    added = block.add(node.record());
                    assert added;
                }

            }
            //compress and write
            if (block.entryCount() > 0) {
                if (records.isFull()) {
                    flushRecords(records);
                }
                inserted += block.entryCount();
                block.write(records);
                block.clear();
            }

            if (!records.isEmpty()) {
                flushRecords(records);
            }

            memTable.clear();
            return inserted;
        }
    }


}
