package io.joshworks.fstore.log.segment.header;

public enum Type {
    OPEN(0),
    EMPTY(1),
    LOG_HEAD(2),
    MERGE_OUT(3),
    READ_ONLY(4);

    final int val;

    Type(int i) {
        this.val = i;
    }

    static Type of(int type) {
        for (Type theType : Type.values()) {
            if (theType.val == type) {
                return theType;
            }
        }
        throw new IllegalArgumentException("Invalid type: " + type);

    }

}
