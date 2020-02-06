package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import static io.joshworks.ilog.lsm.Block2.keyOverhead;
import static java.util.Objects.requireNonNull;

class MemTable {

    final ByteBuffer[] table;
    private final KeyComparator comparator;
    private final BufferPool recordPool; //used to store records
    private final AtomicInteger entries = new AtomicInteger();
    private final AtomicInteger sizeInBytes = new AtomicInteger();

    private final ByteBuffer tmpKey;

    private final StampedLock lock = new StampedLock();

    MemTable(KeyComparator comparator, int maxEntries, int maxRecordSize) {
        this.table = new ByteBuffer[maxEntries];
        this.recordPool = BufferPool.defaultPool(maxEntries, maxRecordSize, true);
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
            int entries = this.entries.get();
            if (entries == 0 || compareLast(record, keyOffset) > 0) {
                table[entries] = memRec;
                this.entries.incrementAndGet();
                sizeInBytes.addAndGet(recordSize);
                return;
            }

            int idx = binarySearch(memRec, keyOffset);

            updateTable(memRec, recordSize, idx);

        } catch (Exception e) {
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    private int compareLast(ByteBuffer record, int keyOffset) {
        ByteBuffer last = table[entries.get() - 1];
        int kOffset = Record2.KEY.offset(last);
        return comparator.compare(record, keyOffset, last, kOffset);
    }

    private void updateTable(ByteBuffer memRec, int recordSize, int idx) {
        long stamp = lock.writeLock();
        try {
            if (idx >= 0) { //existing entry
                ByteBuffer existing = table[idx];
                int existingSize = Record2.sizeOf(existing);
                table[idx] = memRec;
                int diff = recordSize - existingSize;
                sizeInBytes.addAndGet(diff);
            } else {
                idx = Math.abs(idx) - 1;
                System.arraycopy(table, idx,
                        table, idx + 1,
                        entries.get() - idx);
                table[idx] = memRec;
                entries.incrementAndGet();
                sizeInBytes.addAndGet(recordSize);
            }
        } finally {
            lock.unlockWrite(stamp);
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

        ByteBuffer record = table[idx];
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
        if (entries.get() == 0) {
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
//        table.clear(); // TODO remove
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
        int entries = this.entries.get();

        int low = 0;
        int high = entries - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            ByteBuffer rec = table[mid];
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
