package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.LogIterator;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.pooled.HeapBlock;
import io.joshworks.ilog.pooled.ObjectPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Lsm {

    public static final String LOG_DIR = "log";
    public static final String SSTABLES_DIR = "sstables";

    public final SequenceLog tlog;
    private final MemTable memTable;
    private final Log<SSTable> ssTables;

    private final BufferPool recordPool;

    private final long maxAge;

    private final ObjectPool<HeapBlock> blockPool;
    private final RowKey rowKey;


    Lsm(File root,
        RowKey rowKey,
        int memTableMaxSizeInBytes,
        int memTableMaxEntries,
        boolean directBuffers,
        int blockSize,
        long maxAge,
        int compactionThreads,
        int compactionThreshold,
        Codec codec) throws IOException {
        this.rowKey = rowKey;

        FileUtils.createDir(root);
        this.maxAge = maxAge;

        this.blockPool = new ObjectPool<>(100, p -> new HeapBlock(p, blockSize, rowKey, directBuffers, codec));

//        int maxRecordSize = Record2.HEADER_BYTES + comparator.keySize() + blockSize;
        int maxRecordSize = 36 + rowKey.keySize() + blockSize;

        this.recordPool = BufferPool.localCachePool(256, maxRecordSize, directBuffers);
        BufferPool logRecordPool = BufferPool.localCachePool(256, maxRecordSize, directBuffers);

//        int sstableIndexSize = memTableMaxEntries * (keySize + Long.BYTES); //key + pos
        int sstableIndexSize = memTableMaxSizeInBytes; // FIXME this needs to be properly calculated
        int tlogIndexSize = sstableIndexSize * 4;

        this.memTable = new MemTable(memTableMaxEntries);

        this.tlog = new SequenceLog(new File(root, LOG_DIR),
                tlogIndexSize,
                2,
                1,
                FlushMode.ON_ROLL,
                logRecordPool);

        this.ssTables = new Log<>(new File(root, SSTABLES_DIR),
                sstableIndexSize,
                compactionThreshold,
                compactionThreads,
                FlushMode.ON_ROLL,
                recordPool,
                (file, idxSize) -> new SSTable(file, idxSize, rowKey));
    }

    public static Builder create(File root, RowKey comparator) {
        return new Builder(root, comparator);
    }

    public void append(Records records) {
        tlog.append(records);
        writeToMemTable(records);
    }

    private void writeToMemTable(Records records) {
        int inserted = 0;
        while (inserted < records.size()) {
            int i = memTable.add(records, inserted);
            if (i == 0) {
                flush();
            }
            inserted += i;
        }
    }

    public int get(ByteBuffer key, ByteBuffer dst) {
        if (rowKey.keySize() != key.remaining()) {
            throw new IllegalArgumentException("Invalid key size");
        }
        return ssTables.apply(Direction.BACKWARD, sst -> {

            int fromMem = memTable.apply(key, dst, IndexFunctions.EQUALS);
            if (fromMem > 0) {
                return fromMem;
            }

            var record = recordPool.allocate();
            try (HeapBlock block = blockPool.allocate()) {
                for (SSTable ssTable : sst) {
                    record.clear();
                    if (!ssTable.readOnly()) {
                        block.clear();
                        continue;
                    }
                    int read = ssTable.find(key, record, IndexFunctions.FLOOR);
                    if (read > 0) {
                        record.flip();
                        int entrySize = readFromBlock(key, block, record, dst, IndexFunctions.EQUALS);
                        if (entrySize <= 0) {// not found in the block, continue
                            block.clear();
                            continue;
                        }
                        return entrySize;
                    }
                }
                return 0;
            } finally {
                recordPool.free(record);
            }
        });
    }

    private int readFromBlock(ByteBuffer key, HeapBlock heapBlock, ByteBuffer record, ByteBuffer dst, IndexFunctions func) {

        assert Record.isValid(record);

        int blockStart = Record.VALUE.offset(record);
        int blockSize = Record.VALUE.len(record);

        Buffers.offsetPosition(record, blockStart);
        Buffers.offsetLimit(record, blockSize);
        heapBlock.from(record, true);

        return heapBlock.find(key, dst, func);
    }

    public int readLog(ByteBuffer dst, long id) {
        return tlog.bulkRead(id, dst, IndexFunctions.EQUALS);
    }

    public LogIterator logIterator() {
        return tlog.iterator();
    }

    public LogIterator logIterator(long fromSequence) {
        return tlog.iterator(fromSequence);
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
