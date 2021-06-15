package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.ObjectPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Segment;
import io.joshworks.ilog.SegmentFactory;
import io.joshworks.ilog.compaction.combiner.DiscardCombiner;
import io.joshworks.ilog.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Block;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.nio.ByteBuffer;

public class Lsm {

    public static final String LOG_DIR = "log";
    public static final String SSTABLES_DIR = "sstables";

    protected final Log<Segment> tlog;
    protected final MemTable memTable;
    protected final Log<SSTable> ssTables;

    protected final RecordPool pool;

    private final long maxAge;
    protected final RowKey rowKey;

    private final ObjectPool<Block> blockPool;

    Lsm(File root,
        RecordPool pool,
        RowKey rowKey,
        int memTableMaxEntries,
        int memTableMaxSize,
        boolean memTableDirectBuffer,
        long maxAge,
        int compactionThreshold,
        int blockSize,
        Codec codec) {

        FileUtils.createDir(root);
        this.pool = pool;
        this.maxAge = maxAge;
        this.rowKey = rowKey;

        // FIXME index can hold up to Integer.MAX_VALUE which probably isn't enough for large dataset
        this.memTable = new MemTable(pool, rowKey, memTableMaxEntries, memTableMaxSize, memTableDirectBuffer);
        this.tlog = new Log<>(
                new File(root, LOG_DIR),
                Size.MB.of(20),
                Segment.NO_MAX_ENTRIES,
                -1,
                new DiscardCombiner(),
                FlushMode.ON_ROLL,
                pool,
                Segment::new);


        this.ssTables = new Log<>(
                new File(root, SSTABLES_DIR),
                Segment.NO_MAX_SIZE,
                memTableMaxEntries,
                compactionThreshold,
                new UniqueMergeCombiner(pool, rowKey),
                FlushMode.ON_ROLL,
                pool,
                SegmentFactory.sstable(rowKey));

        this.blockPool = new ObjectPool<>(p -> new Block(p, pool, blockSize, rowKey, codec));
    }

    public static Builder create(File root, RowKey comparator) {
        return new Builder(root, comparator);
    }

    public void append(Records records) {
        tlog.append(records);
        Records.RecordIterator it = records.iterator();
        while (!memTable.add(it)) {
            flush();
        }
    }

    public Record get(ByteBuffer key) {
        if (rowKey.keySize() != key.remaining()) {
            throw new IllegalArgumentException("Invalid key size");
        }
        return ssTables.apply(Direction.BACKWARD, sst -> {
            Record fromMem = memTable.find(key, IndexFunction.EQUALS);
            if (fromMem != null) {
                return fromMem;
            }

            for (SSTable ssTable : sst) {
                if (!ssTable.readOnly()) {
                    continue;
                }
                Record found = searchSSTable(key, ssTable, IndexFunction.EQUALS);
                if (found != null) {
                    return found;
                }
            }
            return null;
        });
    }

    private Record searchSSTable(ByteBuffer key, SSTable ssTable, IndexFunction func) {
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

    //TODO implement iterator, after adding the data block to memtable
    //then track the position on the memtable, once is flushed, fallback to disk by using offset

    public synchronized void flush() {
        if (memTable.isEmpty()) {
            return;
        }
        int memTableSize = memTable.size();
        long inserted = flushMemTable();

        assert inserted == memTableSize;
        assert memTable.isEmpty();
        assert memTable.size() == 0;

        if (inserted > 0) {
            ssTables.roll();
        }
    }

    protected Block readBlock(SSTable ssTable, ByteBuffer key) {
        Record blockRec = ssTable.find(key, IndexFunction.FLOOR);
        if (blockRec == null) {
            return null;
        }
        Block block = blockPool.allocate();
        block.from(blockRec);
        return block;

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

    protected void flushRecords(Records records) {
        assert !records.isEmpty();
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
