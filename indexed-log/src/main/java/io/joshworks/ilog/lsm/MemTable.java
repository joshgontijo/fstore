package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.lsm.tree.RedBlackBST;
import io.joshworks.ilog.pooled.HeapBlock;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

class MemTable {

    private final RedBlackBST table;
    private final ByteBuffer data;
    private final KeyComparator comparator;
    private final ByteBuffer tmpKey;
    private final int keySize;

    private final StampedLock lock = new StampedLock();

    MemTable(KeyComparator comparator, int memTableSizeInBytes, int maxEntries, boolean direct) {
        this.table = new RedBlackBST(comparator, maxEntries, direct);
        this.data = Buffers.allocate(memTableSizeInBytes, direct);
        this.comparator = comparator;
        this.tmpKey = Buffers.allocate(comparator.keySize(), direct);
        this.keySize = comparator.keySize();
    }

    boolean add(ByteBuffer record) {
        requireNonNull(record, "Record must be provided");
        try {

            if (data.remaining() < record.remaining() || table.isFull()) {
                return false;
            }
            int recordPos = data.position();
            int recordLen = Record.sizeOf(record);

            long stamp = lock.writeLock();
            try {
                int copied = Buffers.copy(record, data);
                if (recordLen != copied) {
                    throw new IllegalStateException("Unexpected record length");
                }
                table.put(record, recordPos);
            } finally {
                lock.unlockWrite(stamp);
            }
            return true;

        } catch (Exception e) {
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    public int apply(ByteBuffer key, ByteBuffer dst, IndexFunctions fn) {
        validateKeySize(key);

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

        Node node = table.get(key);
        if (node == null) {
            return 0;
        }
        return Buffers.copy(data, node.offset(), node.recordLen(), dst);
    }

    public int size() {
        return table.size();
    }

    private void validateKeySize(ByteBuffer key) {
        if (key.remaining() != comparator.keySize()) {
            throw new IllegalArgumentException("Invalid key size: " + key.remaining());
        }
    }

    //TODO implement maxAge
    long writeTo(Consumer<ByteBuffer> writer, long maxAge, HeapBlock block) {
        if (table.isEmpty()) {
            return 0;
        }

        long inserted = 0;

        for (Node node : table) {
            int recordOffset = node.offset();
            int recordLen = node.recordLen();

            boolean added = block.add(data, recordOffset, recordLen);
            if (!added) {
                inserted += block.entryCount();
                block.write(writer);
                block.clear();

                boolean added1 = block.add(data, recordOffset, recordLen);
                assert added1;
            }

        }
        //compress and write
        if (block.entryCount() > 0) {
            inserted += block.entryCount();
            block.write(writer);
            block.clear();
        }

        assert inserted == table.size();

        // TODO remove
        table.clear();
        data.clear();
        return inserted;
    }
}
