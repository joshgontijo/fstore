package io.joshworks.fstore.log.server.tcp;

public class Append {

    public String key;
    public byte[] record;

    public Append() {
    }

    public Append(String key, byte[] record) {
        this.key = key;
        this.record = record;
    }
}
