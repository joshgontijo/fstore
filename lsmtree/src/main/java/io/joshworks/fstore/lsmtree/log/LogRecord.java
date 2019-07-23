package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

public class LogRecord<K, V> {

    public final EntryType type;
    public final K key;
    public final V value;

    private LogRecord(EntryType type, K key, V value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public static <K, V> LogRecord<K, V> add(K key, V value) {
        return new LogRecord<>(EntryType.ADD, key, value);
    }

    public static <K, V> LogRecord<K, V> delete(K key) {
        return new LogRecord<>(EntryType.DELETE, key, null);
    }

    static <K, V> LogRecord<K, V> memFlushed() {
        return new LogRecord<>(EntryType.MEM_FLUSHED, null, null);
    }

}
