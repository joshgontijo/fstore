package io.joshworks.fstore.log.server;

public class LogRecord {

    public final long sequence;
    public final long timestamp;
    public final byte[] data;


    public LogRecord(long sequence, long timestamp, byte[] data) {
        this.sequence = sequence;
        this.timestamp = timestamp;
        this.data = data;
    }
}
