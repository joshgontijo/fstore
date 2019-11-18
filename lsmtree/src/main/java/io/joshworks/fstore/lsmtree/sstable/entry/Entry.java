package io.joshworks.fstore.lsmtree.sstable.entry;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Entry<K extends Comparable<K>, V> implements Comparable<Entry<K, V>> {

    public static final long NO_TIMESTAMP = -1;
    public static final long NO_MAX_AGE = -1;

    public final long timestamp; //in seconds
    public final K key;
    public final V value;

    private Entry(long timestamp, K key, V value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public static <K extends Comparable<K>, V> Entry<K, V> of(long timestamp, K key, V value) {
        return new Entry<>(timestamp, key, value);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> key(K key) {
        requireNonNull(key, "Key must be provided");
        return of(NO_TIMESTAMP, key, null);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> add(K key, V value) {
        requireNonNull(key, "Key must be provided");
        requireNonNull(value, "Value must be provided");
        return of(nowSeconds(), key, value);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> delete(K key) {
        requireNonNull(key, "Key must be provided");
        return new Entry<>(nowSeconds(), key, null);
    }

    public boolean deletion() {
        return value == null;
    }

    public boolean expired(long maxAgeSeconds) {
        long now = nowSeconds();
        return maxAgeSeconds > 0 && timestamp > NO_TIMESTAMP && (now - timestamp > maxAgeSeconds);
    }

    private static long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    public boolean readable(long maxAge) {
        return !deletion() && !expired(maxAge);
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
        return deletion() ? "DELETE: " + key : "ADD:" + key + "=" + value;
    }
}
