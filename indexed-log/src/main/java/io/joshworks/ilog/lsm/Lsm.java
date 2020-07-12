package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Segment;
import io.joshworks.ilog.SegmentFactory;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.polled.ObjectPool;
import io.joshworks.ilog.record.Block;
import io.joshworks.ilog.record.RecordIterator;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.nio.ByteBuffer;

public class Lsm {

    public static final String LOG_DIR = "log";
    public static final String SSTABLES_DIR = "sstables";

    private final Log<Segment> tlog;
    private final MemTable memTable;
    private final Log<SSTable> ssTables;

    private final RecordPool pool;

    private final long maxAge;

    private final ObjectPool<Block> blockPool;
    private final RowKey rowKey;


    Lsm(File root,
        RowKey rowKey,
        int memTableMaxSizeInBytes,
        int memTableMaxEntries,
        boolean memTableDirectBuffers,
        int blockSize,
        long maxAge,
        int compactionThreads,
        int compactionThreshold,
        Codec codec) {

        FileUtils.createDir(root);
        this.maxAge = maxAge;
        this.rowKey = rowKey;

        this.pool = RecordPool.create()
                .directBuffers(memTableDirectBuffers)
                .build();

        RecordPool sstablePool = RecordPool.create()
                .directBuffers(memTableDirectBuffers)
                .build();

        this.blockPool = new ObjectPool<>(100, p -> new Block(pool, blockSize, rowKey, codec));

        // FIXME index can hold up to Integer.MAX_VALUE which probably isn't enough for large dataset

        this.memTable = new MemTable(pool, rowKey, memTableMaxEntries);
        this.tlog = new Log<>(
                new File(root, LOG_DIR),
                memTableMaxEntries, //
                2,
                1,
                FlushMode.ON_ROLL,
                pool,
                Segment::new);


        this.ssTables = new Log<>(new File(root, SSTABLES_DIR),
                memTableMaxSizeInBytes,
                compactionThreshold,
                compactionThreads,
                FlushMode.ON_ROLL,
                sstablePool,
                SegmentFactory.sstable(rowKey, memTableMaxEntries, blockPool));
    }

    public static Builder create(File root, RowKey comparator) {
        return new Builder(root, comparator);
    }

    public void append(Records records) {
        tlog.append(records);
        RecordIterator it = records.iterator();
        while (it.hasNext()) {
            memTable.add(it);
            if (memTable.isFull()) {
                flush();
            }
        }
    }

    public Records get(ByteBuffer key) {
        if (rowKey.keySize() != key.remaining()) {
            throw new IllegalArgumentException("Invalid key size");
        }
        return ssTables.apply(Direction.BACKWARD, sst -> {
            Records fromMem = memTable.find(key, IndexFunction.EQUALS);
            if (fromMem != null) {
                return fromMem;
            }

            for (SSTable ssTable : sst) {
                Records found = ssTable.find(key, IndexFunction.FLOOR);
                if (found != null) {
                    return found;
                }
            }
            return null;
        });
    }

    public synchronized void flush() {
        long entries = flushMemTable();
        if (entries > 0) {
            ssTables.roll();
        }
    }

    private long flushMemTable() {
        if (memTable.isEmpty()) {
            return 0;
        }

        long inserted = 0;

        Records records = pool.empty();

        try (Block block = blockPool.allocate()) {
            for (Node node : memTable) {
                boolean added = block.add(node.record());
                if (!added) {
                    if (records.isFull()) {
                        flushBlockRecords(records);
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
                    flushBlockRecords(records);
                }
                inserted += block.entryCount();
                block.write(records);
                block.clear();
            }

            if (records.size() > 0) {
                flushBlockRecords(records);
            }

            assert inserted == memTable.size();

            memTable.clear();
            return inserted;
        }
    }

    private void flushBlockRecords(Records records) {
        ssTables.append(records);
        records.clear();
    }

    public void delete() {
        tlog.delete();
        ssTables.delete();
    }

    public void close() {
        tlog.close();
        ssTables.close();
    }

}
