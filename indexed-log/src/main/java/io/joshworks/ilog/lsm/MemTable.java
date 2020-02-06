package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import static io.joshworks.ilog.lsm.Block2.keyOverhead;
import static java.util.Objects.requireNonNull;

class MemTable {

    final List<ByteBuffer> table;
    private final KeyComparator comparator;
    private final BufferPool recordPool; //used to store records
    private final AtomicInteger entries = new AtomicInteger();
    private final AtomicInteger sizeInBytes = new AtomicInteger();

    private final ByteBuffer tmpKey;

    private final StampedLock lock = new StampedLock();

    MemTable(KeyComparator comparator, int maxEntries, int maxRecordSize) {
        this.table = new ArrayList<>(maxEntries);
        this.recordPool = BufferPool.defaultPool(maxEntries, maxRecordSize, false);
        this.comparator = comparator;
        this.tmpKey = Buffers.allocate(comparator.keySize(), false);
    }

    void add(ByteBuffer record) {
        requireNonNull(record, "Record must be provided");

        var memRec = recordPool.allocate();
        try {
            Buffers.copy(record, memRec);
            memRec.flip();

            int keyOffset = Record2.KEY.offset(memRec);
            int recordSize = Record2.sizeOf(memRec);
            int idx = binarySearch(memRec, keyOffset);

            updateTable(memRec, recordSize, idx);

        } catch (Exception e) {
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    private void updateTable(ByteBuffer memRec, int recordSize, int idx) {
        long stamp = lock.writeLock();
        try {
            if (idx >= 0) { //existing entry
                ByteBuffer existing = table.get(idx);
                int existingSize = Record2.sizeOf(existing);
                table.set(idx, memRec);
                int diff = recordSize - existingSize;
                sizeInBytes.addAndGet(diff);
            } else {
                table.add(Math.abs(idx) - 1, memRec);
                entries.incrementAndGet();
            }
        } finally {
            lock.tryUnlockWrite();
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
        int idx = binarySearch(key, key.position());
        idx = fn.apply(idx);
        if (idx < 0) {
            return 0;
        }

        ByteBuffer record = table.get(idx);
        return LsmRecord.fromRecord(record, dst);
    }

    public int size() {
        return entries.get();
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
        for (ByteBuffer record : table) {
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

        for (ByteBuffer buffer : table) {
            recordPool.free(buffer);
        }


        entries.set(0); // TODO remove
        table.clear(); // TODO remove
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

    private int binarySearch(ByteBuffer key, int keyStart) {
        int entries = table.size();

        int low = 0;
        int high = entries - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            ByteBuffer rec = table.get(mid);
            int cmp = comparator.compare(rec, 0, key, keyStart);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

}
