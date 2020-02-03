package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.joshworks.ilog.lsm.Block2.keyOverhead;
import static java.util.Objects.requireNonNull;

class MemTable {

    final ConcurrentSkipListMap<ByteBuffer, ByteBuffer> table;
    private final KeyComparator comparator;
    private final BufferPool keyPool;
    private final AtomicInteger entries = new AtomicInteger();
    private final AtomicInteger sizeInBytes = new AtomicInteger();

    MemTable(KeyComparator comparator, BufferPool keyPool) {
        this.table = new ConcurrentSkipListMap<>(comparator);
        this.comparator = comparator;
        this.keyPool = keyPool;
    }

    int add(ByteBuffer record) {
        requireNonNull(record, "Record must be provided");
        var keyBuffer = keyPool.allocate();
        try {
            Record2.writeKey(record, keyBuffer);
            keyBuffer.flip();
            validateKeySize(keyBuffer);
            ByteBuffer existing = table.put(keyBuffer, record);
            sizeInBytes.addAndGet(record.remaining());

            if (existing != null) {
                sizeInBytes.addAndGet(-existing.remaining());
                return entries.get();
            }
            return entries.incrementAndGet();

        } catch (Exception e) {
            keyPool.free(keyBuffer);
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    public int get(ByteBuffer key, ByteBuffer dst) {
        validateKeySize(key);
        var floorEntry = table.floorEntry(key);
        if (floorEntry == null) {
            return 0;
        }

        int compare = comparator.compare(floorEntry.getKey(), key);
        if (compare != 0) {
            return 0;
        }
        return Buffers.copy(floorEntry.getValue(), dst);
    }

    public ByteBuffer floor(ByteBuffer key) {
        var entry = table.floorEntry(key);
        return getValue(entry);
    }

    public ByteBuffer ceiling(ByteBuffer key) {
        var entry = table.ceilingEntry(key);
        return getValue(entry);
    }

    public ByteBuffer higher(ByteBuffer key) {
        var entry = table.higherEntry(key);
        return getValue(entry);
    }

    public ByteBuffer lower(ByteBuffer key) {
        var entry = table.lowerEntry(key);
        return getValue(entry);
    }

    public int size() {
        return entries.get();
    }

    private void validateKeySize(ByteBuffer key) {
        if (key.remaining() != comparator.keySize()) {
            throw new IllegalArgumentException("Invalid key size: " + key.remaining());
        }
    }

    private static ByteBuffer getValue(Map.Entry<ByteBuffer, ByteBuffer> entry) {
        return entry == null ? null : entry.getValue();
    }

    long writeTo(Consumer<ByteBuffer> writer, long maxAge, Codec codec, ByteBuffer block, ByteBuffer blockRecords, ByteBuffer dst) {
        if (table.isEmpty()) {
            return 0;
        }

        long inserted = 0;
        int blockEntries = 0;
        ByteBuffer firstBlockKey = null;
        int keyOverhead = keyOverhead(comparator.keySize());

        Buffers.offsetPosition(block, Block2.HEADER_SIZE);
        for (Map.Entry<ByteBuffer, ByteBuffer> kv : table.entrySet()) {
            ByteBuffer key = kv.getKey();
            ByteBuffer record = kv.getValue();
            if (LsmRecord.expired(record, maxAge) && !LsmRecord.deletion(record)) {
                continue;
            }

            int valueRegionSize = blockRecords.position();
            int valueSize = Record2.valueSize(record);
            int valueOverhead = Block2.Record.valueOverhead(valueSize);

            //not enough space in the block, compress and write
            if (block.remaining() < valueRegionSize + keyOverhead + valueOverhead) {
                writeBlock(writer, codec, block, blockRecords, dst, blockEntries, firstBlockKey);

                //reset state
                blockEntries = 0;
                firstBlockKey = null;

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
            Buffers.copy(key, block);
            block.putInt(offset);

            firstBlockKey = firstBlockKey == null ? key : firstBlockKey;
            blockEntries++;
            inserted++;
        }
        writeBlock(writer, codec, block, blockRecords, dst, blockEntries, firstBlockKey);

        entries.set(0); // TODO remove
        table.clear(); // TODO remove
        return inserted;
    }

    private void writeBlock(Consumer<ByteBuffer> writer, Codec codec, ByteBuffer block, ByteBuffer blockRecords, ByteBuffer dst, int blockEntries, ByteBuffer firstBlockKey) {
        blockRecords.flip();
        int uncompressedSize = blockRecords.remaining();

        if (block.remaining() < uncompressedSize) {
            throw new IllegalStateException("Not enough space block space");
        }

        Block2.compress(blockRecords, block, codec);
        block.flip();

        Block2.writeHeader(block, uncompressedSize, blockEntries);

        Record2.create(firstBlockKey, block, dst);
        dst.flip();

        writer.accept(dst);
    }

}
