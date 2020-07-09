package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.polled.ObjectPool;
import io.joshworks.ilog.record.BufferRecords;
import io.joshworks.ilog.record.HeapBlock;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Lsm {

    public static final String LOG_DIR = "log";
    public static final String SSTABLES_DIR = "sstables";

    private final Log<IndexedSegment> tlog;
    private final MemTable memTable;
    private final Log<SSTable> ssTables;

    private final RecordPool pool;

    private final long maxAge;

    private final ObjectPool<HeapBlock> blockPool;
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
        Codec codec) throws IOException {

        FileUtils.createDir(root);
        this.maxAge = maxAge;
        this.rowKey = rowKey;

        this.pool = RecordPool.create(rowKey)
                .directBuffers(memTableDirectBuffers)
                .build();

        RecordPool sstablePool = RecordPool.create(rowKey)
                .directBuffers(memTableDirectBuffers)
                .build();

        this.blockPool = new ObjectPool<>(100, p -> new HeapBlock(pool, blockSize, rowKey, codec));

        // FIXME index can hold up to Integer.MAX_VALUE which probably isn't enough for large dataset

        this.memTable = new MemTable(pool, memTableMaxEntries);
        this.tlog = new Log<>(
                new File(root, LOG_DIR),
                memTableMaxEntries, //
                2,
                1,
                FlushMode.ON_ROLL,
                pool,
                IndexedSegment::new);


        this.ssTables = new Log<>(new File(root, SSTABLES_DIR),
                memTableMaxSizeInBytes,
                compactionThreshold,
                compactionThreads,
                FlushMode.ON_ROLL,
                sstablePool,
                (file, indexEntries, pool) -> new SSTable(file, indexEntries, pool, blockPool));
    }

    public static Builder create(File root, RowKey comparator) {
        return new Builder(root, comparator);
    }

    public void append(BufferRecords records) {
        Records copy = records.copy(); //copy so it can be reused in memtable
        tlog.append(records);
        while (copy.hasNext()) {
            memTable.add(copy);
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
            Records fromMem = memTable.apply(key, IndexFunction.EQUALS);
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
        try (HeapBlock block = blockPool.allocate()) {
            long entries = memTable.writeTo(ssTables::append, block);
            if (entries > 0) {
                ssTables.roll();
            }
        }
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
