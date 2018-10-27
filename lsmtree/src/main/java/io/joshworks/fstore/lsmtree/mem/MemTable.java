package io.joshworks.fstore.lsmtree.mem;

import io.joshworks.fstore.lsmtree.sstable.Entry;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class MemTable<K extends Comparable<K>, V> {

    public final int flushThreshold;
    private final Map<K, Entry<K, V>> table = new HashMap<>();

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

    public Stream<Entry<K, V>> sorted() {
        return table.values().stream().sorted(Comparator.comparing(e -> e.key));
    }

    public void clear() {
        table.clear();
    }

}
