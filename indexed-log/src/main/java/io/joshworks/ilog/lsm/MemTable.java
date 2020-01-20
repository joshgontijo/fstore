package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

class MemTable {

    private final ConcurrentSkipListSet<ByteBuffer> table;
    private final KeyComparator comparator;
    private final BufferPool keyPool;
    private final AtomicInteger size = new AtomicInteger();

    MemTable(KeyComparator comparator, BufferPool keyPool) {
        this.table = new ConcurrentSkipListSet<>(bufferComparator(comparator));
        this.comparator = comparator;
        this.keyPool = keyPool;
    }

//    private Comparator<Record> recordComparator(KeyComparator comparator) {
//        return (r1, r2) -> {
//            ByteBuffer k1 = keyPool.allocate();
//            ByteBuffer k2 = keyPool.allocate();
//            try {
//                r1.readKey(k1);
//                k1.flip();
//
//                r2.readKey(k2);
//                k2.flip();
//
//                return comparator.compare(k1, k2);
//            } finally {
//                keyPool.free(k1);
//                keyPool.free(k2);
//            }
//        };
//    }

    private Comparator<ByteBuffer> bufferComparator(KeyComparator comparator) {
        return (r1, r2) -> {
            ByteBuffer k1 = null;
            ByteBuffer k2 = null;
            try {
                k1 = keyOf(r1, keyPool, comparator);
                k2 = keyOf(r2, keyPool, comparator);
                return comparator.compare(k1, k2);
            } finally {
                k1.flip();
                k2.flip();
                if (k1 != r1) {
                    keyPool.free(k1);
                }
                if (k2 != r2) {
                    keyPool.free(k2);
                }
            }
        };
    }

    private static ByteBuffer keyOf(ByteBuffer buffer, BufferPool pool, KeyComparator comparator) {
        if (buffer.remaining() != comparator.keySize()) {
            ByteBuffer recordKey = pool.allocate();
            Record.readKey(buffer, recordKey, comparator.keySize());
            return recordKey.flip();
        }
        return buffer;
    }

    int add(Record record) {
        requireNonNull(record, "Record must be provided");
        ByteBuffer buffer = record.buffer;
        boolean added = table.add(buffer);
        if (!added) {
            table.remove(buffer);
            table.add(buffer);
            return size.get();
        }
        return size.incrementAndGet();
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


        for (int i = 0; i < 10; i++) {
            ByteBuffer key = ByteBuffer.allocate(Long.BYTES).putLong(i).flip();
            ByteBuffer floor = table.floor(key);

            Record record = Record.from(floor, false);
            String toString = record.toString(Serializers.LONG, Serializers.STRING);
            System.out.println(toString);
        }


    }

    private static Record create(long key, String value) {
        return Record.create(key, Serializers.LONG, value, Serializers.STRING, ByteBuffer.allocate(64));
    }

    //    @Override
//    public Record get(ByteBuffer key) {
//        if (key.remaining() != comparator.keySize()) {
//            throw new IllegalArgumentException("Invalid key size");
//        }
//        ByteBuffer floor = floor(key);
//        return found.key.equals(key) ? found : null;
//    }

    //    @Override
    public ByteBuffer floor(ByteBuffer key) {
        return table.floor(key);
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
