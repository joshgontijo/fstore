package io.joshworks.fstore.lsmtree.log;

import java.util.Objects;
import java.util.UUID;

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

    static LogRecord memFlushed(String token) {
        return new IndexFlushed(timeInSeconds(), token);
    }

    static IndexFlushedStarted memFlushStarted(long position) {
        String token = UUID.randomUUID().toString().substring(0, 8);
        return new IndexFlushedStarted(timeInSeconds(), position, token);
    }

    private static long timeInSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogRecord logRecord = (LogRecord) o;
        return timestamp == logRecord.timestamp &&
                type == logRecord.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, timestamp);
    }
}
