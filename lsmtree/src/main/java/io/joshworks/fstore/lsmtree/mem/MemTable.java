package io.joshworks.fstore.lsmtree.mem;

import io.joshworks.fstore.lsmtree.sstable.Entry;

import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable<K extends Comparable<K>, V> {

    private final SortedMap<K, Entry<K, V>> table = new TreeMap<>();

    public void add(K key, V value) {
        table.put(key, Entry.add(key, value));
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

    public Collection<Entry<K, V>> sorted() {
        return table.values();
    }

    public Map<K, Entry<K, V>> copy() {
        return new TreeMap<>(table);
    }

    public void clear() {
        table.clear();
    }

    public int size() {
        return table.size();
    }
}
