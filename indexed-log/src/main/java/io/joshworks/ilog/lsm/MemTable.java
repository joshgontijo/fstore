package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

class MemTable {

    private final ConcurrentSkipListMap<ByteBuffer, ByteBuffer> table;
    private final KeyComparator comparator;
    private final BufferPool keyPool;
    private final AtomicInteger size = new AtomicInteger();

    MemTable(KeyComparator comparator, BufferPool keyPool) {
        this.table = new ConcurrentSkipListMap<>(comparator);
        this.comparator = comparator;
        this.keyPool = keyPool;
    }

    int add(Record record) {
        requireNonNull(record, "Record must be provided");
        var keyBuffer = keyPool.allocate();
        try {
            var recordBuffer = record.buffer;
            record.readKey(keyBuffer);
            keyBuffer.flip();
            validateKeySize(keyBuffer);
            ByteBuffer existing = table.put(keyBuffer, recordBuffer);
            if (existing != null) {
                return size.get();
            }
            return size.incrementAndGet();

        } catch (Exception e) {
            keyPool.free(keyBuffer);
            throw new RuntimeException("Failed to insert record", e);
        }
    }

    public static void main(String[] args) {

        MemTable table = new MemTable(KeyComparator.LONG, BufferPool.unpooled(16, false));
        for (int i = 0; i < 10; i++) {
            table.add(create(i, String.valueOf(i)));
        }

//        for (ByteBuffer bb : table.table) {
//            //FIXME relative positioning on Record.from, what to do ?
//            Record record = Record.from(bb, false);
//            String toString = record.toString(Serializers.LONG, Serializers.STRING);
//            System.out.println(toString);
//        }


//        for (int i = 0; i < 10; i++) {
//            ByteBuffer key = ByteBuffer.allocate(Long.BYTES).putLong(i).flip();
//            ByteBuffer floor = table.floor(key);
//
//            Record record = Record.from(floor, false);
//            String toString = record.toString(Serializers.LONG, Serializers.STRING);
//            System.out.println(toString);
//        }

        System.out.println("--------");
        for (int i = 0; i < 10; i++) {
            ByteBuffer key = ByteBuffer.allocate(Long.BYTES).putLong(i).flip();
            ByteBuffer floor = table.get(key);

            Record record = Record.from(floor, false);
            String toString = record.toString(Serializers.LONG, Serializers.STRING);
            System.out.println(toString);
        }

    }

    private static Record create(long key, String value) {
        return Record.create(key, Serializers.LONG, value, Serializers.STRING, ByteBuffer.allocate(64));
    }

    public ByteBuffer get(ByteBuffer key) {
        validateKeySize(key);
        var floor = floor(key);
        if (floor == null) {
            return null;
        }
        return compareRecord(floor, key) == 0 ? floor : null;
    }

    //    @Override
    public ByteBuffer floor(ByteBuffer key) {
        var entry = table.floorEntry(key);
        return getValue(entry);
    }

    private int compareRecord(ByteBuffer record, ByteBuffer key) {
        var keyBuffer = keyPool.allocate();
        try {
            return Record.compareKey(key, record, keyBuffer, comparator);
        } finally {
            keyPool.free(keyBuffer);
        }
    }

    private void validateKeySize(ByteBuffer key) {
        if (key.remaining() != comparator.keySize()) {
            throw new IllegalArgumentException("Invalid key size: " + key.remaining());
        }
    }

    private static ByteBuffer getValue(Map.Entry<ByteBuffer, ByteBuffer> entry) {
        return entry == null ? null : entry.getValue();
    }

//
//    @Override
//    public Entry<K, V> ceiling(K key) {
//        return table.ceiling(Entry.key(key));
//    }
//
//    @Override
//    public Entry<K, V> higher(K key) {
//        return table.higher(Entry.key(key));
//    }
//
//    @Override
//    public Entry<K, V> lower(K key) {
//        return table.lower(Entry.key(key));
//    }
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
//    long writeTo(LogAppender<Entry<K, V>> sstables, long maxAge) {
//        if (isEmpty()) {
//            return 0;
//        }
//
//        long inserted = 0;
//        for (Entry<K, V> entry : table) {
//            if (entry.expired(maxAge) && !entry.deletion()) {
//                continue;
//            }
//            long entryPos = sstables.append(entry);
//            inserted++;
//            if (entryPos == Storage.EOF) {
//                sstables.roll();
//            }
//        }
//        sstables.roll();
//        return inserted;
//    }
//
//    int size() {
//        return table.size();
//    }
//
//    boolean isEmpty() {
//        return table.isEmpty();
//    }

}
