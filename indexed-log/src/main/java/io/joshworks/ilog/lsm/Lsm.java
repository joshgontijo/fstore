package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
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
    private final KeyComparator keyComparator;
    private final Codec codec;

    private final BufferPool keyPool;
    private final BufferPool recordPool;
    private final BufferPool logRecordPool;

    private final int memTableSize;
    private final long maxAge;
    private final int blockSize;

    private final ByteBuffer writeBlock;
    private final BufferPool writeBlockPool;
    private final BufferPool readBlockPool;


    public Lsm(File root, KeyComparator keyComparator, int maxEntrySize, int memTableEntries, int blockSize, long maxAge) throws IOException {
        this.keyComparator = keyComparator;
        FileUtils.createDir(root);
        this.blockSize = blockSize;
        this.memTableSize = memTableEntries;
        this.maxAge = maxAge;
        this.keyPool = BufferPool.localCachePool(memTableEntries * 2, keyComparator.keySize(), false);
        this.recordPool = BufferPool.localCachePool(256, maxEntrySize, false);
        this.logRecordPool = BufferPool.localCachePool(256, maxEntrySize, false);

        int sstableIndexSize = memTableEntries * (keyComparator.keySize() + Long.BYTES); //key + pos
        int tlogIndexSize = sstableIndexSize * 4;

        this.tlog = new SequenceLog(new File(root, LOG_DIR), maxEntrySize, tlogIndexSize, 2, FlushMode.ON_ROLL, logRecordPool);
        this.memTable = new MemTable(keyComparator, keyPool);
        this.ssTables = new Log<>(new File(root, SSTABLES_DIR), maxEntrySize, sstableIndexSize, 2, FlushMode.ON_ROLL, recordPool, (file, idxSize) -> new SSTable(file, idxSize, keyComparator));
    }

    public void append(ByteBuffer record) {
        tlog.append(record);
        if (memTable.add(record) >= memTableSize) {
            flush();
        }
    }

    public int get(ByteBuffer key, ByteBuffer dst) {
        return ssTables.apply(Direction.BACKWARD, sst -> {
            int fromMem = memTable.get(key, dst);
            if (fromMem > 0) {
                return fromMem;
            }

            var blockBuffer = readBlockPool.allocate();
            try {
                for (SSTable ssTable : sst) {
                    if (!ssTable.readOnly()) {
                        continue;
                    }
                    int fromDisk = ssTable.apply(key, blockBuffer, IndexFunctions.FLOOR);
                    if (fromDisk > 0) {
                        int entrySize = readFromBlock(key, blockBuffer, dst);
                        if (entrySize <= 0) {// not found in the block, continue
                            continue;
                        }
                        return entrySize;
                    }
                }
                return 0;
            } finally {
                readBlockPool.free(blockBuffer);
            }
        });
    }

    private int readFromBlock(ByteBuffer key, ByteBuffer compressedBlock, ByteBuffer dst) {
        compressedBlock.flip();
        int keyIdx = Block2.binarySearch(compressedBlock, key, keyComparator);
        if (keyIdx < 0) {
            return 0;
        }
        return decompressAndRead(dst, compressedBlock, keyIdx);
    }

    private int decompressAndRead(ByteBuffer entryDst, ByteBuffer compressedBlock, int keyIdx) {
        ByteBuffer blockBuffer = readBlockPool.allocate();
        try {
            Block2.decompress(compressedBlock, codec, blockBuffer);
            blockBuffer.flip();
            return Block2.read(compressedBlock, keyIdx, entryDst);
        } finally {
            readBlockPool.free(blockBuffer);
        }
    }

    void flush() {
        memTable.writeTo(ssTables, maxAge);
    }

    public void delete() {
        tlog.delete();
        ssTables.delete();
    }
}
