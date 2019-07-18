package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class MemTable<K extends Comparable<K>, V> {

    private final ConcurrentSkipListSet<Entry<K, V>> table = new ConcurrentSkipListSet<>();
    private final AtomicInteger size = new AtomicInteger();

    public int add(K key, V value) {
        nonNullKey(key);
        Entry<K, V> entry = Entry.add(key, value);
        boolean added = table.add(entry);
        if (!added) {
            table.remove(entry);
            table.add(entry);
            return size.get();
        }
        return size.incrementAndGet();
    }

    public boolean delete(K key) {
        nonNullKey(key);
        return table.remove(Entry.key(key));
    }

    public V get(K key) {
        nonNullKey(key);
        Entry<K, V> found = floor(key);
        if (found == null) {
            return null;
        }
        return found.key.equals(key) ? found.value : null;
    }

    public Entry<K, V> floor(K key) {
        return table.floor(Entry.key(key));
    }

    public Entry<K, V> ceiling(K key) {
        return table.ceiling(Entry.key(key));
    }

    public Entry<K, V> higher(K key) {
        return table.higher(Entry.key(key));
    }

    public Entry<K, V> lower(K key) {
        return table.lower(Entry.key(key));
    }

    public Iterator<Entry<K, V>> iterator() {
        return table.iterator();
    }

    public void writeTo(SSTables<K, V> sstables) {
        if (isEmpty()) {
            return;
        }

        for (Entry<K, V> entry : table) {
            long entryPos = sstables.write(entry);
            if (entryPos == Storage.EOF) {
                sstables.roll();
            }
        }
        sstables.roll();
    }

    public LogIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        NavigableSet<Entry<K, V>> subSet = table.subSet(Entry.key(range.start()), Entry.key(range.end()));
        return Direction.BACKWARD.equals(direction) ? Iterators.wrap(subSet.descendingIterator()) : Iterators.of(subSet);
    }

    public int size() {
        return table.size();
    }

    public boolean isEmpty() {
        return table.isEmpty();
    }

    private void nonNullKey(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must be provided");
        }
    }

}