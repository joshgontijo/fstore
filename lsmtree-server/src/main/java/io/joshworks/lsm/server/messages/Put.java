package io.joshworks.lsm.server.messages;

public class Put {

    public String namespace;
    public String key;
    public byte[] value;

    public Put(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public Put() {
    }

    @Override
    public String toString() {
        return "Put{" +
                "key='" + key + '\'' +
                '}';
    }
}
