package io.joshworks.ilog.lsm;

import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.lsm.tree.RedBlackBST;
import io.joshworks.ilog.record.BufferRecords;
import io.joshworks.ilog.record.HeapBlock;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

class MemTable {

    private final RedBlackBST table = new RedBlackBST();

    private final StampedLock lock = new StampedLock();
    private final int maxEntries;
    private final RecordPool pool;

    MemTable(RecordPool pool, int maxEntries) {
        this.pool = pool;
        this.maxEntries = maxEntries;
    }

    void add(Records records) {
        requireNonNull(records, "Records must be provided");
        try {
            long stamp = lock.writeLock();
            try {
                while (records.hasNext() && !isFull()) {
                    Record2 record = records.poll();
                    table.put(record);
                }
            } finally {
                lock.unlockWrite(stamp);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    public Records apply(ByteBuffer key, IndexFunction fn) {

        long stamp = lock.tryOptimisticRead();
        Records read = tryRead(key, fn);
        if (lock.validate(stamp)) {
            return read;
        }
        if (read != null) {
            read.close();
        }

        stamp = lock.readLock();
        try {
            return tryRead(key, fn);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private Records tryRead(ByteBuffer key, IndexFunction fn) {
        Node node = table.apply(key, fn);
        if (node == null) {
            return null;
        }

        BufferRecords recs = pool.empty();
        recs.add(node.record());

        return recs;
    }

    public int size() {
        return table.size();
    }

    long writeTo(Consumer<Records> writer, HeapBlock block) {
        if (table.isEmpty()) {
            return 0;
        }

        long inserted = 0;

        BufferRecords records = pool.empty();

        for (Node node : table) {

            boolean added = block.add(node.record());
            if (!added) {
                if (records.isFull()) {
                    flushBlockRecords(writer, records);
                }

                inserted += block.entryCount();
                block.write(records);
                block.clear();


                added = block.add(node.record());
                assert added;
            }

        }
        //compress and write
        if (block.entryCount() > 0) {
            if (records.isFull()) {
                flushBlockRecords(writer, records);
            }
            inserted += block.entryCount();
            block.write(records);
            block.clear();
        }

        if (records.size() > 0) {
            flushBlockRecords(writer, records);
        }

        assert inserted == table.size();

        // TODO remove
        table.clear();
        return inserted;
    }

    private void flushBlockRecords(Consumer<Records> writer, BufferRecords records) {
        writer.accept(records);
        records.clear();
    }

    public boolean isFull() {
        return table.size() >= maxEntries;
    }
}
