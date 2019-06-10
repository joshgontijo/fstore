package io.joshworks.fstore.log.segment;

public enum WriteMode {

    LOG_HEAD(0),
    MERGE_OUT(1);

    public final int val;

    WriteMode(int i) {
        this.val = i;
    }

    public static WriteMode of(int type) {
        for (WriteMode theType : WriteMode.values()) {
            if (theType.val == type) {
                return theType;
            }
        }
        throw new IllegalArgumentException("Invalid type: " + type);

    }
}
