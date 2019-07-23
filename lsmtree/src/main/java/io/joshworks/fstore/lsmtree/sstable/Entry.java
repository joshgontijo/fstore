package io.joshworks.fstore.lsmtree.sstable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Entry<K extends Comparable<K>, V> implements Comparable<Entry<K, V>> {

    public final K key;
    public final V value;

    private Entry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public static <K extends Comparable<K>, V> Entry<K, V> of(K key, V value) {
        return new Entry<>(key, value);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> key(K key) {
        requireNonNull(key, "Key must be provided");
        return of(key, null);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> add(K key, V value) {
        requireNonNull(key, "Key must be provided");
        requireNonNull(value, "Value must be provided");
        return of(key, value);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> delete(K key) {
        requireNonNull(key, "Key must be provided");
        return new Entry<>(key, null);
    }

    public boolean deletion() {
        return value == null;
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
        return deletion() ? "DELETE" : "ADD" + ":" + key + "=" + value;
    }
}
