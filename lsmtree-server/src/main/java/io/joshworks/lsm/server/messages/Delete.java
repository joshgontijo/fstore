package io.joshworks.lsm.server.messages;

public class Delete {

    public String namespace;
    public String key;

    public Delete(String key) {
        this.key = key;
    }

    public Delete() {
    }
}
