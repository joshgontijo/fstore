package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

public class LogRecord {

    public final EntryType type;
    public final long timestamp;

    LogRecord(EntryType type, long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    public static <K, V> LogRecord add(K key, V value) {
        return new EntryAdded<>(timeInSeconds(), key, value);
    }

    public static <K> LogRecord delete(K key) {
        return new EntryDeleted<>(timeInSeconds(), key);
    }

    static LogRecord memFlushed(long position) {
        return new IndexFlushed(timeInSeconds(), position);
    }

    private static long timeInSeconds() {
        return System.currentTimeMillis() / 1000;
    }

}
