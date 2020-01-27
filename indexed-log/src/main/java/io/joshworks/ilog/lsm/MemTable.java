package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.KeyComparator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

class MemTable {

    final ConcurrentSkipListMap<ByteBuffer, ByteBuffer> table;
    private final KeyComparator comparator;
    private final BufferPool keyPool;
    private final AtomicInteger size = new AtomicInteger();

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
            if (existing != null) {
                return size.get();
            }
            return size.incrementAndGet();

        } catch (Exception e) {
            keyPool.free(keyBuffer);
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    public ByteBuffer get(ByteBuffer key) {
        validateKeySize(key);
        var floorRecord = floor(key);
        if (floorRecord == null) {
            return null;
        }
        int compare = Record2.compareToKey(floorRecord, key, comparator);
        return compare == 0 ? floorRecord : null;
    }

    //    @Override
    public ByteBuffer floor(ByteBuffer key) {
        var entry = table.floorEntry(key);
        return getValue(entry);
    }

    private void validateKeySize(ByteBuffer key) {
        if (key.remaining() != comparator.keySize()) {
            throw new IllegalArgumentException("Invalid key size: " + key.remaining());
        }
    }

    private static ByteBuffer getValue(Map.Entry<ByteBuffer, ByteBuffer> entry) {
        return entry == null ? null : entry.getValue();
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
        return size.get();
    }

//
//    CloseableIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
//        NavigableSet<Entry<K, V>> subSet;
//        if (range.start() != null && range.end() == null) {
//            subSet = table.tailSet(Entry.key(range.start()), true);
//        } else if (range.start() == null && range.end() != null) {
//            subSet = table.headSet(Entry.key(range.end()), false);
//        } else if (range.start() != null && range.end() != null) {
//            subSet = table.subSet(Entry.key(range.start()), Entry.key(range.end()));
//        } else {
//            throw new IllegalArgumentException("Range start or end must be provided");
//        }
//        return Direction.FORWARD.equals(direction) ? Iterators.of(subSet) : Iterators.wrap(subSet.descendingIterator());
//    }
//
//    CloseableIterator<Entry<K, V>> iterator(Direction direction) {
//        return Direction.FORWARD.equals(direction) ? Iterators.of(table) : Iterators.wrap(table.descendingIterator());
//    }
//
    long writeTo(Log<SSTable> sstables, long maxAge) {
        if (table.isEmpty()) {
            return 0;
        }

        long inserted = 0;
        for (ByteBuffer entry : table.values()) {
            if (entry.expired(maxAge) && !entry.deletion()) {
                continue;
            }
            sstables.append(entry);
        }
        sstables.roll();
        return inserted;
    }
//
//    int size() {
//        return table.size();
//    }
//
//    boolean isEmpty() {
//        return table.isEmpty();
//    }

}
