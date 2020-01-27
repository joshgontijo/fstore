package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.index.KeyComparator;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Lsm {

    private final SequenceLog tlog;
    private final MemTable memTable;
    private final Log<SSTable> ssTables;

    private final BufferPool keyPool;
    private final BufferPool recordPool;
    private final BufferPool logRecordPool;
    private final int memTableSize;
    private final long maxAge;

    public Lsm(File root, KeyComparator keyComparator, int maxEntrySize, int tlogIndexSize, int memTableSize, long maxAge) throws IOException {
        this.memTableSize = memTableSize;
        this.maxAge = maxAge;
        this.keyPool = BufferPool.localCachePool(memTableSize * 2, keyComparator.keySize(), false);
        this.recordPool = BufferPool.localCachePool(256, maxEntrySize, false);
        this.logRecordPool = BufferPool.localCachePool(256, maxEntrySize, false);

        this.tlog = new SequenceLog(new File(root, "log"), maxEntrySize, tlogIndexSize, 2, FlushMode.ON_ROLL, logRecordPool);
        this.memTable = new MemTable(keyComparator, keyPool);
        int sstableIndexSize = memTableSize * keyComparator.keySize();
        this.ssTables = new Log<>(new File(root, "sstables"), maxEntrySize, sstableIndexSize, 2, FlushMode.ON_ROLL, recordPool, (file, idxSize) -> new SSTable(file, idxSize, keyComparator));
    }

    public void put(ByteBuffer record) {
        tlog.append(record);
        if (memTable.add(record) >= memTableSize) {
            flush();
        }
    }

    public void delete(ByteBuffer key) {

    }

    public void read(ByteBuffer key, ByteBuffer dst) {
        ssTables.apply(sst -> {
            ByteBuffer buff = memTable.get(key);
            if(buff)
            for (SSTable ssTable : sst) {
                if()
            }

        })
    }

    private void flush() {
        memTable.writeTo(ssTables, maxAge);
    }

}
