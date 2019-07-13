package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

public class LogEntry<K extends Comparable<K>, V> implements Comparable<LogEntry<K, V>> {

    public final EntryType type;
    public final K key;
    public final V value;

    private LogEntry(EntryType type, K key, V value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public static <K extends Comparable<K>, V> LogEntry<K, V> key(K key) {
        return of(null, key, null);
    }

    public static <K extends Comparable<K>, V> LogEntry<K, V> of(EntryType type, K key, V value) {
        return new LogEntry<>(type, key, value);
    }

    public static <K extends Comparable<K>, V> LogEntry<K, V> add(K key, V value) {
        return of(EntryType.ADD, key, value);
    }

    public static <K extends Comparable<K>, V> LogEntry<K, V> delete(K key) {
        return of(EntryType.DELETE, key, null);
    }

    @Override
    public int compareTo(LogEntry<K, V> o) {
        return key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return type + ":" + key + "=" + value;
    }
}
