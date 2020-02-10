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

    long writeTo(Consumer<ByteBuffer> writer, long maxAge, Codec codec, HeapBlock block, ByteBuffer blockBuffer, ByteBuffer blockRecords, ByteBuffer dst) {
        if (table.isEmpty()) {
            return 0;
        }

        long inserted = 0;

        for (Node node : table) {
            int recordOffset = node.offset();
            int recordLen = node.recordLen();

            Buffers.view(data, recordOffset, recordLen); //view of a Record
            if (RecordFlags.expired(data, maxAge) && !RecordFlags.deletion(data)) {
                continue;
            }

            int keySize = Record.KEY_LEN.get(data);

            int sizeOfRecord = Record.sizeOf(data);

            assert keySize == this.keySize;
            assert recordLen == sizeOfRecord;

            if (blockRecords.remaining() < sizeOfRecord) {
                //writeBLock
                blockRecords.flip();
                inserted += write(writer, codec, block, blockBuffer, blockRecords, dst);

                //clear buffers
                blockRecords.clear();
                blockBuffer.clear();
                dst.clear();
            }
            assert Record.isValid(data);
            int copied = Buffers.copy(data, recordOffset, recordLen, blockRecords);
            assert copied == sizeOfRecord;
        }
        //compress and write
        if (blockRecords.position() > 0) {
            //writeBLock
            blockRecords.flip();
            inserted += write(writer, codec, block, blockBuffer, blockRecords, dst);
        }

        assert inserted == table.size();

        // TODO remove
        table.clear();
        data.clear();
        return inserted;
    }

    private int write(Consumer<ByteBuffer> writer, Codec codec, HeapBlock block, ByteBuffer blockBuffer, ByteBuffer blockRecords, ByteBuffer dst) {
        HeapBlock.create(block, blockRecords, codec);
        block.copyTo(blockBuffer);
        blockBuffer.flip();

        Record.create(block.first(), blockBuffer, dst);
        dst.flip();

        writer.accept(dst);
        return block.entries();
    }

    private ByteBuffer copyKey(ByteBuffer record) {
        Record.KEY.copyTo(record, tmpKey);
        tmpKey.flip();
        return tmpKey;
    }


}
