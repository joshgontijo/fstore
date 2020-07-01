package io.joshworks.ilog.lsm;

import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.lsm.tree.RedBlackBST;
import io.joshworks.ilog.pooled.HeapBlock;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.Records;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

class MemTable {

    private final RedBlackBST table = new RedBlackBST();

    private final StampedLock lock = new StampedLock();
    private final int maxEntries;

    MemTable(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    int add(Records records, int offset) {
        requireNonNull(records, "Records must be provided");
        try {
            long stamp = lock.writeLock();
            try {
                int inserted = 0;
                for (int idx = offset; idx < records.size(); idx++, inserted++) {
                    if (table.size() >= maxEntries) {
                        return inserted;
                    }
                    Record2 record = records.get(idx);
                    table.put(record);
                }
                return inserted;
            } finally {
                lock.unlockWrite(stamp);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    public int apply(ByteBuffer key, ByteBuffer dst, IndexFunctions fn) {

        long stamp = lock.tryOptimisticRead();
        int ppos = dst.position();
        int read = tryRead(key, dst, fn);
        if (lock.validate(stamp)) {
            return read;
        }

        dst.position(ppos);//reset to previous position

        stamp = lock.readLock();
        try {
            return tryRead(key, dst, fn);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private int tryRead(ByteBuffer key, ByteBuffer dst, IndexFunctions fn) {
        Node node = table.apply(key, fn);
        if (node == null) {
            return 0;
        }
        return node.record().copy(dst);
    }

    public int size() {
        return table.size();
    }

    long writeTo(Consumer<Records> writer, HeapBlock block) {
        if (table.isEmpty()) {
            return 0;
        }

        long inserted = 0;

        for (Node node : table) {

            boolean added = block.add(node.record());
            if (!added) {
                inserted += block.entryCount();
//                block.compress();
                block.write(writer);
                block.clear();

                added = block.add(node.record());
                assert added;
            }

        }
        //compress and write
        if (block.entryCount() > 0) {
            inserted += block.entryCount();
//            block.compress();
            block.write(writer);
            block.clear();
        }

        assert inserted == table.size();

        // TODO remove
        table.clear();
        return inserted;
    }
}
