package io.joshworks.fstore.tcp.internal;

public class Ping {

    public final long timestamp;

    public Ping() {
        this.timestamp = System.currentTimeMillis();
    }
}
