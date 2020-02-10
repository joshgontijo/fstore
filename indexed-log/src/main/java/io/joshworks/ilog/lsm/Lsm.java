package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Lsm {

    public static final String LOG_DIR = "log";
    public static final String SSTABLES_DIR = "sstables";

    private final SequenceLog tlog;
    private final MemTable memTable;
    private final Log<SSTable> ssTables;
    private final KeyComparator comparator;
    private final Codec codec;

    private final BufferPool recordPool;
    private final BufferPool blockRecordsBufferPool;

    private final long maxAge;

    private final ByteBuffer writeBlock;
    private final ByteBuffer blockRecords;
    private final ByteBuffer recordBuffer;

    private final HeapBlock heapBlock;


    Lsm(File root,
        KeyComparator comparator,
        int memTableMaxSizeInBytes,
        int memTableMaxEntries,
        boolean directMemTable,
        int blockSize,
        long maxAge,
        int compactionThreads,
        int compactionThreshold,
        Codec codec) throws IOException {

        FileUtils.createDir(root);
        this.comparator = comparator;
        this.maxAge = maxAge;
        this.codec = codec;

        this.heapBlock = new HeapBlock(comparator.keySize(), blockSize);

//        int maxRecordSize = Record2.HEADER_BYTES + comparator.keySize() + blockSize;
        int maxRecordSize = 36 + comparator.keySize() + blockSize;

        this.writeBlock = Buffers.allocate(blockSize, false);
        this.blockRecords = Buffers.allocate(blockSize, false);
        this.recordBuffer = Buffers.allocate(maxRecordSize, false);

        this.blockRecordsBufferPool = BufferPool.localCache(blockSize, false);
        this.recordPool = BufferPool.localCachePool(256, maxRecordSize, false);
        BufferPool logRecordPool = BufferPool.localCachePool(256, maxRecordSize, false);

//        int sstableIndexSize = memTableMaxEntries * (keySize + Long.BYTES); //key + pos
        int sstableIndexSize = memTableMaxSizeInBytes; // FIXME this needs to be properly calculated
        int tlogIndexSize = sstableIndexSize * 4;

        this.memTable = new MemTable(comparator, memTableMaxSizeInBytes, memTableMaxEntries, directMemTable);

        this.tlog = new SequenceLog(new File(root, LOG_DIR),
                maxRecordSize,
                tlogIndexSize,
                0,
                1,
                FlushMode.ON_ROLL,
                logRecordPool);

        this.ssTables = new Log<>(new File(root, SSTABLES_DIR),
                maxRecordSize,
                sstableIndexSize,
                compactionThreshold,
                compactionThreads,
                FlushMode.ON_ROLL,
                recordPool,
                (file, idxSize) -> new SSTable(file, idxSize, comparator));
    }

    public static Builder create(File root, KeyComparator comparator) {
        return new Builder(root, comparator);
    }

    public void append(ByteBuffer record) {
        tlog.append(record);
        if (!memTable.add(record)) {
            flush();
            if (!memTable.add(record)) {
                throw new IllegalStateException("Failed to write to memtable");
            }
        }
    }

    public int get(ByteBuffer key, ByteBuffer dst) {
        return ssTables.apply(Direction.BACKWARD, sst -> {
            int fromMem = memTable.apply(key, dst, IndexFunctions.EQUALS);
            if (fromMem > 0) {
                return fromMem;
            }

            var record = recordPool.allocate();
            try {
                for (SSTable ssTable : sst) {
                    record.clear();
                    if (!ssTable.readOnly()) {
                        continue;
                    }
                    int read = ssTable.find(key, record, IndexFunctions.FLOOR);
                    if (read > 0) {
                        record.flip();
                        int entrySize = readFromBlock(key, heapBlock, record, dst, IndexFunctions.EQUALS);
                        if (entrySize <= 0) {// not found in the block, continue
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
        ByteBuffer decompressedTmp = blockRecordsBufferPool.allocate();
        try {
            int blockStart = Record.VALUE.offset(record);
            int blockSize = Record.VALUE.len(record);
            heapBlock.readFrom(record, blockStart);

            int offset = heapBlock.binarySearch(key, comparator, func);
            if (offset < 0) {
                return 0;
            }
            heapBlock.decompress(decompressedTmp, codec);
            decompressedTmp.flip();

            assert offset < decompressedTmp.remaining();

            decompressedTmp.position(offset);
            return Record.copyTo(decompressedTmp, dst);
        } finally {
            blockRecordsBufferPool.free(decompressedTmp);
        }
    }

    synchronized void flush() {
        writeBlock.clear();
        blockRecords.clear();
        recordBuffer.clear();
        long entries = memTable.writeTo(ssTables::append, maxAge, codec,heapBlock, writeBlock, blockRecords, recordBuffer);
        if (entries > 0) {
            ssTables.roll();
        }
    }

    public void delete() {
        tlog.delete();
        ssTables.delete();
    }

}
