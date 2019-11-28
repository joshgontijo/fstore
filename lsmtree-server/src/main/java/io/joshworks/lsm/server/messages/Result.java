package io.joshworks.lsm.server.messages;

import java.util.Arrays;

public class Result {

    public byte[] key;
    public byte[] value;

    public Result(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public Result() {
    }

    @Override
    public String toString() {
        return "Result{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}
