package io.joshworks.ilog.lsm;

import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.lsm.tree.RedBlackBST;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordIterator;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.locks.StampedLock;

import static java.util.Objects.requireNonNull;

public class MemTable implements Iterable<Node> {

    private final RedBlackBST table;

    private final StampedLock lock = new StampedLock();
    private final int maxEntries;
    private final RecordPool pool;

    public MemTable(RecordPool pool, RowKey rowKey, int maxEntries) {
        this.pool = pool;
        this.maxEntries = maxEntries;
        this.table = new RedBlackBST(rowKey);
    }

    public void add(RecordIterator records) {
        requireNonNull(records, "Records must be provided");
        try {
            long stamp = lock.writeLock();
            try {
                while (records.hasNext() && !isFull()) {
                    Record record = records.next();
                    table.put(record);
                }
            } finally {
                lock.unlockWrite(stamp);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    public Records find(ByteBuffer key, IndexFunction fn) {

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

        Records recs = pool.empty();
        recs.add(node.record());

        return recs;
    }

    public int size() {
        return table.size();
    }

    public boolean isFull() {
        return table.size() >= maxEntries;
    }

    @Override
    public Iterator<Node> iterator() {
        return table.iterator();
    }

    public boolean isEmpty() {
        return table.isEmpty();
    }

    public void clear() {
        table.clear();
    }
}
