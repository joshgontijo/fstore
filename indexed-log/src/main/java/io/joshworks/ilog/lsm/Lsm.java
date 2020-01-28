package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
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

    private final BufferPool keyPool;
    private final BufferPool recordPool;
    private final BufferPool logRecordPool;
    private final int memTableSize;
    private final long maxAge;

    public Lsm(File root, KeyComparator keyComparator, int maxEntrySize, int memTableEntries, long maxAge) throws IOException {
        FileUtils.createDir(root);
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
            for (SSTable ssTable : sst) {
                if (!ssTable.readOnly()) {
                    continue;
                }
                int fromDisk = ssTable.get(key, dst);
                if (fromDisk > 0) {
                    return fromDisk;
                }
            }
            return 0;
        });
    }

    void flush() {
        memTable.writeTo(ssTables, maxAge);
    }

    public void delete() {
        tlog.delete();
        ssTables.delete();
    }
}
