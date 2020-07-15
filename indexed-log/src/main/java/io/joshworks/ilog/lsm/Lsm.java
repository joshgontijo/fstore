package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Segment;
import io.joshworks.ilog.SegmentFactory;
import io.joshworks.ilog.compaction.combiner.DiscardCombiner;
import io.joshworks.ilog.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class Lsm {

    public static final String LOG_DIR = "log";
    public static final String SSTABLES_DIR = "sstables";

    protected final Log<Segment> tlog;
    protected final MemTable memTable;
    protected final Log<SSTable> ssTables;

    protected final RecordPool pool;

    private final long maxAge;
    private final RowKey rowKey;

    Lsm(File root,
        RecordPool pool,
        RowKey rowKey,
        int memTableMaxEntries,
        long maxAge,
        int compactionThreshold) {

        FileUtils.createDir(root);
        this.pool = pool;
        this.maxAge = maxAge;
        this.rowKey = rowKey;

        // FIXME index can hold up to Integer.MAX_VALUE which probably isn't enough for large dataset

        this.memTable = new MemTable(pool, rowKey, memTableMaxEntries);
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
    }

    public static Builder create(File root, RowKey comparator) {
        return new Builder(root, comparator);
    }

    public void append(Records records) {
        tlog.append(records);
        Iterator<Record> it = records.iterator();
        while (it.hasNext()) {
            memTable.add(it);
            if (memTable.isFull()) {
                flush();
            }
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

    protected Record searchSSTable(ByteBuffer key, SSTable ssTable, IndexFunction func) {
        return ssTable.find(key, func);
    }

    public synchronized void flush() {
        if (memTable.isEmpty()) {
            return;
        }
        int memTableSize = memTable.size();
        long inserted = flushMemTable();
        assert inserted == memTableSize;
        if (inserted > 0) {
            ssTables.roll();
        }
    }

    protected long flushMemTable() {
        long inserted = 0;

        try(Records records = pool.empty()) {
            for (Node node : memTable) {
                boolean added = records.add(node.record());
                if (!added) {
                    inserted += records.size();
                    flushRecords(records);
                    records.add(node.record());
                }
            }
            if (!records.isEmpty()) {
                inserted += records.size();
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
