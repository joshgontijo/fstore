package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.lsmtree.Range;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;

import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

class MemTable<K extends Comparable<K>, V> implements TreeFunctions<K, V> {

    private final ConcurrentSkipListSet<Entry<K, V>> table = new ConcurrentSkipListSet<>();
    private final AtomicInteger size = new AtomicInteger();

    MemTable() {

    }

    int add(Entry<K, V> entry) {
        requireNonNull(entry, "Entry must be provided");
        boolean added = table.add(entry);
        if (!added) {
            table.remove(entry);
            table.add(entry);
            return size.get();
        }
        return size.incrementAndGet();
    }

    @Override
    public Entry<K, V> get(K key) {
        requireNonNull(key, "Key must be provided");
        Entry<K, V> found = floor(key);
        if (found == null) {
            return null;
        }
        return found.key.equals(key) ? found : null;
    }

    @Override
    public Entry<K, V> floor(K key) {
        return table.floor(Entry.key(key));
    }

    @Override
    public Entry<K, V> ceiling(K key) {
        return table.ceiling(Entry.key(key));
    }

    @Override
    public Entry<K, V> higher(K key) {
        return table.higher(Entry.key(key));
    }

    @Override
    public Entry<K, V> lower(K key) {
        return table.lower(Entry.key(key));
    }

    CloseableIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        NavigableSet<Entry<K, V>> subSet;
        if (range.start() != null && range.end() == null) {
            subSet = table.tailSet(Entry.key(range.start()), true);
        } else if (range.start() == null && range.end() != null) {
            subSet = table.headSet(Entry.key(range.end()), false);
        } else if (range.start() != null && range.end() != null) {
            subSet = table.subSet(Entry.key(range.start()), Entry.key(range.end()));
        } else {
            throw new IllegalArgumentException("Range start or end must be provided");
        }
        return Direction.FORWARD.equals(direction) ? Iterators.of(subSet) : Iterators.wrap(subSet.descendingIterator());
    }

    CloseableIterator<Entry<K, V>> iterator(Direction direction) {
        return Direction.FORWARD.equals(direction) ? Iterators.of(table) : Iterators.wrap(table.descendingIterator());
    }

    long writeTo(LogAppender<Entry<K, V>> sstables, long maxAge) {
        if (isEmpty()) {
            return 0;
        }

        long inserted = 0;
        for (Entry<K, V> entry : table) {
            if (entry.expired(maxAge) && !entry.deletion()) {
                continue;
            }
            long entryPos = sstables.append(entry);
            inserted++;
            if (entryPos == Storage.EOF) {
                sstables.roll();
            }
        }
        sstables.roll();
        return inserted;
    }

    int size() {
        return table.size();
    }

    boolean isEmpty() {
        return table.isEmpty();
    }

}
