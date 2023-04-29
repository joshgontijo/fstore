package io.joshworks.sdb.server;

public enum OP {

    APPEND((short) 1),
    VERSION((short) 2),
    SUBSCRIBE((short) 3);

    public static int LEN = Short.BYTES;
    public final short val;

    OP(short val) {
        this.val = val;
    }
}
