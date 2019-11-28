package io.joshworks.lsm.server.messages;

import java.util.Arrays;

public class Delete {

    public String namespace;
    public byte[] key;

    public Delete(byte[] key) {
        this.key = key;
    }

    public Delete() {
    }

    @Override
    public String toString() {
        return "Delete{" +
                "namespace='" + namespace + '\'' +
                ", key=" + Arrays.toString(key) +
                '}';
    }
}
