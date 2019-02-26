package io.joshworks.fstore.log.segment.header;

public enum Type {
    LOG_HEAD(2),
    MERGE_OUT(3);

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
