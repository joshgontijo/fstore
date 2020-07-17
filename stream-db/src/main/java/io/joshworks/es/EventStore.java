package io.joshworks.es;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.SSTable;
import io.joshworks.ilog.lsm.SparseLsm;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.record.Block;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.nio.ByteBuffer;

public class EventStore extends SparseLsm {

    private static final int BLOCK_SIZE = 4096;
    private final RecordPool pool = RecordPool.create().build();
    private final RowKey rowKey = new StreamKey();

    protected EventStore(File root, RecordPool pool, RowKey rowKey, int memTableMaxEntries, int memTableMaxSize, boolean memTableDirectBuffer, long maxAge, int compactionThreshold, int blockSize, Codec codec) {
        super(root, pool, rowKey, memTableMaxEntries, memTableMaxSize, memTableDirectBuffer, maxAge, compactionThreshold, blockSize, codec);
    }

    private ByteBuffer of(long stream, int version) {
        ByteBuffer bb = pool.allocate(rowKey.keySize());
        bb.putLong(stream);
        bb.putInt(version);
        return bb.flip();
    }

    private static void incrementVersion(ByteBuffer bb) {
        bb.putInt(Long.BYTES, bb.getInt(Long.BYTES) + 1);
    }


    public Records fromStream(long stream, int startVersion) {
        ByteBuffer key = of(stream, startVersion);

        return ssTables.apply(Direction.FORWARD, sst -> {
            Records records = pool.empty();

            for (SSTable ssTable : sst) {
                if (!ssTable.readOnly()) {
                    continue;
                }

                if (records.isFull()) {
                    return records;
                }

                while(milkSegment(key, records, ssTable)) {

                }
            }

            //now memtable
            fromMemTable(key, records);
            if (records.isFull()) {
                return records;
            }


            return records;
        });
    }

    public boolean milkSegment(ByteBuffer key, Records records, SSTable ssTable) {
        Block block = super.readBlock(ssTable, key);
        if (block == null) { //block not found keep searching
            return false;
        }
        int idx = block.indexOf(key, IndexFunction.EQUALS);
        if (idx == Index.NONE) { //false positive
            return false;
        }
        Record rec = block.read(idx);

        //read block forward for possible next version of the stream
        while (idx < block.entryCount() && rec != null && !records.isFull()) {
            records.add(rec);

            incrementVersion(key);
            int compare = block.compare(idx++, key);
            if (compare == 0) {
                rec = block.read(idx);
            }
        }
        return true;
    }

    public void fromMemTable(ByteBuffer key, Records records) {
        Record found;
        do {
            found = memTable.find(key, IndexFunction.EQUALS);
            if (found != null) {
                records.add(found);
                incrementVersion(key);
            }
        } while (found != null);
    }


    public void append(Records records) {
        log.append(records);
        RecordIterator it = records.iterator();
        while (it.hasNext()) {
            memTable.add(it);
            if (memTable.isFull()) {
                flush();
            }
        }
    }

    public synchronized void flush() {
        for (Node node : memTable) {
            index.write();
        }

    }

}
