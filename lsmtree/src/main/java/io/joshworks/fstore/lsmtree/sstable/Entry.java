package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.lsmtree.EntryType;

public class Entry<K extends Comparable<K>, V> implements Comparable<Entry<K, V>> {

    public final EntryType type;
    public final K key;
    public final V value;

    private Entry(EntryType type, K key, V value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public static <K extends Comparable<K>, V> Entry<K, V> key(K key) {
        return of(null, key, null);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> of(EntryType type, K key, V value) {
        return new Entry<>(type, key, value);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> add(K key, V value) {
        return of(EntryType.ADD, key, value);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> delete(K key) {
        return of(EntryType.DELETE, key, null);
    }

    @Override
    public int compareTo(Entry<K, V> o) {
        return key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return type + ":" + key + "=" + value;
    }
}
