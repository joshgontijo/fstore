package io.joshworks.fstore.log.appender.history.data;

public class StoreCreated {

    public final String name;
    public final String directory;
    public final long timestamp;

    public StoreCreated(String name, String directory, long timestamp) {
        this.name = name;
        this.directory = directory;
        this.timestamp = timestamp;
    }
}
