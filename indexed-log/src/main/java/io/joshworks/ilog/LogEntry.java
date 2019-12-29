package io.joshworks.ilog;

public class LogEntry<T> {

    public final long offset;
    public final long timestamp;
    public final T data;

    public LogEntry(long offset, long timestamp, T data) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.data = data;
    }
}
