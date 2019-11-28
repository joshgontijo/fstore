package io.joshworks.lsm.server.messages;

import java.util.Arrays;

public class Get {

    public String namespace;
    public byte[] key;

    //renames these
    //0 - Any (consider replicas)
    //1 - Local (master only)
    public int readLevel;

    public Get(byte[] key) {
        this.key = key;
    }

    public Get() {
    }

    @Override
    public String toString() {
        return "Get{" +
                "key='" + Arrays.toString(key) + '\'' +
                '}';
    }
}
