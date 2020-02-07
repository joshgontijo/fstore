package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.lsm.tree.RedBlackBST;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import static io.joshworks.ilog.lsm.Block2.keyOverhead;
import static java.util.Objects.requireNonNull;

class MemTable {

    private final RedBlackBST table;
    private final ByteBuffer data;
    private final KeyComparator comparator;

    private final ByteBuffer tmpKey;

    private final StampedLock lock = new StampedLock();

    MemTable(KeyComparator comparator, int memTableSizeInBytes, int maxEntries, boolean direct) {
        this.table = new RedBlackBST(comparator, maxEntries, direct);
        this.data = Buffers.allocate(memTableSizeInBytes, false);
        this.comparator = comparator;
        this.tmpKey = Buffers.allocate(comparator.keySize(), false);
    }

    boolean add(ByteBuffer record) {
        requireNonNull(record, "Record must be provided");

        try {

            if (data.remaining() < record.remaining() || table.isFull()) {
                return false;
            }
            int recordPos = record.position();
            int recordLen = Record2.sizeOf(record);

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
        return Buffers.copy(data, node.offset(), node.len(), dst);
//        ByteBuffer record = table[idx];
        //TODO ------ return LSMRECORD ------
//        return LsmRecord.fromRecord(record, dst);
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

        long inserted = 0;
        int blockEntries = 0;
        ByteBuffer first = null;
        int keyOverhead = keyOverhead(comparator.keySize());

        Buffers.offsetPosition(block, Block2.HEADER_SIZE);

        for (Node node : table) {
            int recordOffset = node.offset();
            int recordLen = node.len();

            ByteBuffer record = data.limit(recordOffset + recordLen).position(recordOffset);

            if (LsmRecord.expired(record, maxAge) && !LsmRecord.deletion(record)) {
                continue;
            }

            int valueRegionSize = blockRecords.position();
            int valueSize = Record2.VALUE_LEN.get(record);
            int valueOverhead = Block2.Record.valueOverhead(valueSize);

            //not enough space in the block, compress and write
            if (block.remaining() < valueRegionSize + keyOverhead + valueOverhead) {
                writeBlock(writer, codec, block, blockRecords, dst, blockEntries, first);

                //reset state
                blockEntries = 0;
                first = null;

                //clear buffers
                block.clear();
                blockRecords.clear();
                dst.clear();
                Buffers.offsetPosition(block, Block2.HEADER_SIZE);
            }


            int offset = blockRecords.position();

            //BLOCK DATA
            Block2.Record.fromRecord(record, blockRecords);

            //KEYS_REGION
            int keyOffset = Record2.KEY.offset(record);
            int keySize = Record2.KEY.len(record);
            Buffers.copy(record, keyOffset, keySize, block);
            block.putInt(offset);

            first = first == null ? record : first;
            blockEntries++;
            inserted++;
        }
        writeBlock(writer, codec, block, blockRecords, dst, blockEntries, first);


        table.clear(); // TODO remove
        data.clear();
        return inserted;
    }

    private void writeBlock(Consumer<ByteBuffer> writer, Codec codec, ByteBuffer block, ByteBuffer blockRecords, ByteBuffer dst, int blockEntries, ByteBuffer firstRecord) {
        blockRecords.flip();
        int uncompressedSize = blockRecords.remaining();

        if (block.remaining() < uncompressedSize) {
            throw new IllegalStateException("Not enough space block space");
        }

        Block2.compress(blockRecords, block, codec);
        block.flip();

        Block2.writeHeader(block, uncompressedSize, blockEntries);

        tmpKey.clear();
        Record2.KEY.copyTo(firstRecord, tmpKey);
        tmpKey.flip();
        Record2.create(tmpKey, block, dst);


        dst.flip();

        writer.accept(dst);
    }
}
