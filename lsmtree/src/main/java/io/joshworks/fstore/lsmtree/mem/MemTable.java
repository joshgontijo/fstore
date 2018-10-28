package io.joshworks.fstore.lsmtree.mem;

import io.joshworks.fstore.lsmtree.sstable.Entry;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class MemTable<K extends Comparable<K>, V> {

    public final int flushThreshold;
    private final SortedMap<K, Entry<K, V>> table = new TreeMap<>();

    public MemTable(int flushThreshold) {
        this.flushThreshold = flushThreshold;
    }

    public boolean add(K key, V value) {
        table.put(key, Entry.add(key, value));
        return table.size() >= flushThreshold;
    }

    public V delete(K key) {
        Entry<K, V> removed = table.remove(key);
        table.put(key, Entry.delete(key));
        return removed != null ? removed.value : null;
    }

    public V get(K key) {
        Entry<K, V> found = table.get(key);
        return found != null ? found.value : null;
    }

    public Stream<Entry<K, V>> streamSorted() {
        return table.values().stream();
    }

    public Map<K, Entry<K, V>> copy() {
        return new TreeMap<>(table);
    }

    public void clear() {
        table.clear();
    }

}
