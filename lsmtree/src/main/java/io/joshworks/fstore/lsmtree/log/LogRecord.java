package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

public class LogRecord<K, V> {

    public final EntryType type;
    public final long timestamp;
    public final K key;
    public final V value;

    private LogRecord(EntryType type, long timestamp, K key, V value) {
        this.type = type;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public static <K, V> LogRecord<K, V> add(K key, V value) {
        return new LogRecord<>(EntryType.ADD, System.currentTimeMillis() / 1000, key, value);
    }

    public static <K, V> LogRecord<K, V> delete(K key) {
        return new LogRecord<>(EntryType.DELETE, System.currentTimeMillis() / 1000, key, null);
    }

    static <K, V> LogRecord<K, V> memFlushed() {
        return new LogRecord<>(EntryType.MEM_FLUSHED, System.currentTimeMillis() / 1000, null, null);
    }

}
