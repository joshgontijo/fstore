package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.util.ObjectPool;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Block;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.nio.ByteBuffer;

public class SparseLsm extends Lsm {

    private final ObjectPool<Block> blockPool;

    protected SparseLsm(File root,
              RecordPool pool,
              RowKey rowKey,
              int memTableMaxEntries,
              int memTableMaxSize,
              boolean memTableDirectBuffer,
              long maxAge,
              int compactionThreshold,
              int blockSize,
              Codec codec) {
        super(root, pool, rowKey, memTableMaxEntries, memTableMaxSize, memTableDirectBuffer, maxAge, compactionThreshold);
        this.blockPool = new ObjectPool<>(p -> new Block(p, pool, blockSize, rowKey, codec));
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

    protected Block readBlock(SSTable ssTable, ByteBuffer key) {
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

        try (Block block = blockPool.allocate(); Records records = pool.empty()) {
            for (Record record : memTable) {
                try (record) {
                    boolean added = block.add(record);
                    if (!added) {
                        if (records.isFull()) {
                            flushRecords(records);
                        }

                        inserted += block.entryCount();
                        block.write(records);
                        block.clear();

                        added = block.add(record);
                        assert added;
                    }
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
