package io.joshworks.fstore.lsmtree.sstable;

import java.util.Objects;

public class Entry<K extends Comparable<K>, V> implements Comparable<Entry<K, V>> {

    public final boolean deletion;
    public final K key;
    public final V value;

    private Entry(boolean deletion, K key, V value) {
        this.deletion = deletion;
        this.key = key;
        this.value = value;
    }

    public static <K extends Comparable<K>, V> Entry<K, V> key(K key) {
        return of(false, key, null);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> of(boolean deletion, K key, V value) {
        return new Entry<>(deletion, key, value);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> add(K key, V value) {
        return of(false, key, value);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> delete(K key) {
        return of(true, key, null);
    }

    public boolean deletion() {
        return deletion;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    @Override
    public int compareTo(Entry<K, V> o) {
        return key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Entry<?, ?> entry = (Entry<?, ?>) o;
        return key.equals(entry.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return deletion ? "DELETE" : "ADD" + ":" + key + "=" + value;
    }
}
