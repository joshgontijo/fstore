package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.lsm.tree.RedBlackBST;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.locks.StampedLock;

import static java.util.Objects.requireNonNull;

public class MemTable implements Iterable<Record> {

    private final RedBlackBST table;

    private final StampedLock lock = new StampedLock();
    private final int maxEntries;
    private final RecordPool pool;

    private final ByteBuffer data;

    public MemTable(RowKey rowKey, int maxEntries, int maxSize, boolean direct) {
        this.pool = pool;
        this.maxEntries = maxEntries;
        this.table = new RedBlackBST(rowKey, maxEntries, direct);
        this.data = Buffers.allocate(maxSize, direct);
    }

    //returns true if all added or records iterator has exhausted
    //false if not all records elements couldn't be added
    public boolean add(ByteBuffer records) {
        requireNonNull(records, "Records must be provided");
        try {
            long stamp = lock.writeLock();
            try {

                while (Record.isValid(records) && !table.isFull()) {
                    Record record = records.peek();
                    if (record.recordSize() > data.remaining()) {
                        return false;
                    }
                    int pos = data.position();
                    record.copyTo(data);
                    records.next(); //advance
                    table.put(record, pos);
                }
                return true;
            } finally {
                lock.unlockWrite(stamp);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    public Record find(ByteBuffer key, IndexFunction fn) {

        long stamp = lock.tryOptimisticRead();
        Record read = tryRead(key, fn);
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

    private Record tryRead(ByteBuffer key, IndexFunction fn) {
        Node node = table.apply(key, fn);
        if (node == null) {
            return null;
        }
        return pool.from(data, node.offset());
    }

    public int size() {
        return table.size();
    }

    @Override
    public Iterator<Record> iterator() {
        return new MemTableIterator(table.iterator());
    }

    public boolean isEmpty() {
        return table.isEmpty();
    }

    public void clear() {
        table.clear();
        data.clear();
    }

    private class MemTableIterator implements Iterator<Record> {

        private final Iterator<Node> tableIterator;

        private MemTableIterator(Iterator<Node> tableIterator) {
            this.tableIterator = tableIterator;
        }

        @Override
        public boolean hasNext() {
            return tableIterator.hasNext();
        }

        @Override
        public Record next() {
            Node node = tableIterator.next();
            return pool.from(data, node.offset());
        }
    }

}
