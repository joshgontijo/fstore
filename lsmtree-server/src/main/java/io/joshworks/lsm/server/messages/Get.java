package io.joshworks.lsm.server.messages;

public class Get {

    public String namespace;
    public String key;

    public Get(String key) {
        this.key = key;
    }

    public Get() {
    }

    @Override
    public String toString() {
        return "Get{" +
                "key='" + key + '\'' +
                '}';
    }
}
