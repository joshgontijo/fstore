package io.joshworks.lsm.server.messages;

public class Result {

    public String key;
    public byte[] value;

    public Result(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public Result() {
    }
}
