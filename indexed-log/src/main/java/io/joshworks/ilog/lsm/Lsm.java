package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Record2;
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

    private final int memTableSize;
    private final long maxAge;

    private final ByteBuffer writeBlock;
    private final ByteBuffer blockRecordRegionBuffer;
    private final ByteBuffer recordBuffer;


    Lsm(File root,
        KeyComparator comparator,
        int memTableEntries,
        int blockSize,
        long maxAge,
        int compactionThreads,
        int compactionThreshold,
        Codec codec) throws IOException {

        FileUtils.createDir(root);
        this.comparator = comparator;
        this.memTableSize = memTableEntries;
        this.maxAge = maxAge;
        this.codec = codec;

        int maxRecordSize = Record2.HEADER_BYTES + comparator.keySize() + blockSize;

        this.writeBlock = Buffers.allocate(blockSize, false);
        this.blockRecordRegionBuffer = Buffers.allocate(blockSize, false);
        this.recordBuffer = Buffers.allocate(maxRecordSize, false);

        int keySize = comparator.keySize();
        int maxEntrySize = Block2.maxEntrySize(blockSize, keySize);
        //Record headers + compressedBlock -> Used only for underlying log

        this.blockRecordsBufferPool = BufferPool.localCache(blockSize, false);
        this.recordPool = BufferPool.localCachePool(256, maxRecordSize, false);
        BufferPool keyPool = BufferPool.defaultPool(memTableEntries * 2, keySize, false);
        BufferPool logRecordPool = BufferPool.localCachePool(256, maxRecordSize, false);

        int sstableIndexSize = memTableEntries * (keySize + Long.BYTES); //key + pos
        int tlogIndexSize = sstableIndexSize * 4;

        this.memTable = new MemTable(comparator, keyPool);

        this.tlog = new SequenceLog(new File(root, LOG_DIR),
                maxRecordSize,
                tlogIndexSize,
                1,
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

    public void append(ByteBuffer lsmRecord) {
        tlog.append(lsmRecord);
        if (memTable.add(lsmRecord) >= memTableSize) {
            flush();
        }
    }

    public int get(ByteBuffer key, ByteBuffer dst) {
        return ssTables.apply(Direction.BACKWARD, sst -> {
            int fromMem = memTable.get(key, dst);
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
                        int entrySize = readFromBlock(key, record, dst, IndexFunctions.EQUALS);
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

    private int readFromBlock(ByteBuffer key, ByteBuffer record, ByteBuffer dst, IndexFunctions func) {
        record.flip();
        int keyIdx = Block2.binarySearch(record, key, func, comparator);
        if (keyIdx < 0) {
            return 0;
        }
        return decompressAndRead(record, dst, keyIdx);
    }

    private int decompressAndRead(ByteBuffer record, ByteBuffer dst, int keyIdx) {
        ByteBuffer blockRecords = blockRecordsBufferPool.allocate();
        try {
            Block2.decompress(record, blockRecords, codec);
            blockRecords.flip();
            return LsmRecord.fromBlockRecord(record, blockRecords, dst, keyIdx, comparator.keySize());
        } finally {
            blockRecordsBufferPool.free(blockRecords);
        }
    }

    synchronized void flush() {
        writeBlock.clear();
        blockRecordRegionBuffer.clear();
        recordBuffer.clear();
        long entries = memTable.writeTo(ssTables::append, maxAge, codec, writeBlock, blockRecordRegionBuffer, recordBuffer);
        if (entries > 0) {
            ssTables.roll();
        }

    }

    public void delete() {
        tlog.delete();
        ssTables.delete();
    }

}
