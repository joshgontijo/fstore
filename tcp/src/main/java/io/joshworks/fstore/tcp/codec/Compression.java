package io.joshworks.fstore.tcp.codec;

public enum Compression {
    NONE(0),
    LZ4_FAST(1),
    LZ4_HIGH(2),
    SNAPPY(3),
    DEFLATE(4);

    private final int val;

    Compression(int val) {
        this.val = val;
    }

    public int val() {
        return val;
    }
}
