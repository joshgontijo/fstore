package io.joshworks.lsm.server.messages;

import java.util.Arrays;

public class Put {

    public String namespace;
    public byte[] key;
    public byte[] value;

    //0 - NO ACK
    //1 - LOCAL
    //2 - QUORUM
    //3 - ALL
    public int replication;

    public Put(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public Put() {
    }

    @Override
    public String toString() {
        return "Put{" +
                "key='" + Arrays.toString(key) + '\'' +
                '}';
    }
}
