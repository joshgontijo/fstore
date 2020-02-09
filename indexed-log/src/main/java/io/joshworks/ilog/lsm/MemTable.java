package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.lsm.tree.RedBlackBST;

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
        this.data = Buffers.allocate(memTableSizeInBytes, false);
        this.comparator = comparator;
        this.tmpKey = Buffers.allocate(comparator.keySize(), false);
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

    long writeTo(Consumer<ByteBuffer> writer, long maxAge, Codec codec, ByteBuffer block, ByteBuffer blockRecords, ByteBuffer dst) {
        if (table.isEmpty()) {
            return 0;
        }

        int maxBlockDataSize = Block.maxCapacity(block);

        long inserted = 0;
        ByteBuffer firstKey = null;
        int computedSize = 0;

        for (Node node : table) {
            int recordOffset = node.offset();
            int recordLen = node.recordLen();

            Buffers.view(data, recordOffset, recordLen); //view of a Record
            if (RecordFlags.expired(data, maxAge) && !RecordFlags.deletion(data)) {
                continue;
            }

            int keySize = Record.KEY_LEN.get(data);
            if (keySize != this.keySize) {
                throw new RuntimeException("Invalid key size");
            }

            int validate = Record.validate(data);

            int entryOverhead = Block.blockEntryOverhead(keySize, recordLen);
            if (computedSize + entryOverhead <= maxBlockDataSize) {
                Buffers.copy(data, blockRecords);
                firstKey = firstKey == null ? copyKey(data) : firstKey;
                computedSize += entryOverhead;
                continue;
            }

            blockRecords.flip();
            computedSize = 0;
            if (firstKey == null) {
                //should never happen
                throw new RuntimeException("Key must not be null");
            }

            //compress and write
            inserted += write(writer, codec, block, blockRecords, dst, firstKey, keySize);

            blockRecords.clear();
            block.clear();
            dst.clear();

            //copy this item to the block
            Buffers.copy(data, recordOffset, recordLen, blockRecords);
            firstKey = copyKey(data);
            computedSize += entryOverhead;

        }
        //compress and write
        if (blockRecords.position() > 0) {
            blockRecords.flip();
            inserted += write(writer, codec, block, blockRecords, dst, firstKey, keySize);
        }

        assert inserted == table.size();

        // TODO remove
        table.clear();
        data.clear();
        return inserted;
    }

    private int write(Consumer<ByteBuffer> writer, Codec codec, ByteBuffer block, ByteBuffer blockRecords, ByteBuffer dst, ByteBuffer firstKey, int keySize2) {
        int entries = Block.create(blockRecords, block, keySize2, codec);
        Record.create(firstKey, block, dst);
        dst.flip();
        writer.accept(dst);
        return entries;
    }

    private ByteBuffer copyKey(ByteBuffer record) {
        Record.KEY.copyTo(record, tmpKey);
        tmpKey.flip();
        return tmpKey;
    }


}
