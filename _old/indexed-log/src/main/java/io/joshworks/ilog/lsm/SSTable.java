package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Block;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;

import java.io.File;
import java.nio.ByteBuffer;

import static io.joshworks.ilog.index.Index.NONE;

public class SSTable extends IndexedSegment {

    private final Cache<Long, Record> cache = Cache.lruCache(1000, -1);

    public SSTable(File file, RecordPool pool, RowKey rowKey, long indexEntries) {
        super(file, pool, rowKey, indexEntries);
    }

    @Override
    public Record find(ByteBuffer key, IndexFunction func) {
        int idx = index.find(key, func);
        if (idx == NONE) {
            return null;
        }
        long pos = index.readPosition(idx);
        int len = index.readEntrySize(idx);

        Record cached = cache.get(pos);
        if (cached != null) {
            return cached;
        }
        Record record = pool.get(channel, pos, len);

        cache.add(pos, record);
        return record;
    }
}
