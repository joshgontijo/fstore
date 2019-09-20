package io.joshworks.fstore.lsmtree;

public enum EntryType {
    MEM_FLUSHED((short) 0),
    DELETE((short) 1),
    ADD((short) 2);

    public final short code;

    EntryType(short code) {
        this.code = code;
    }

    public static EntryType of(short code) {
        return EntryType.values()[code];
    }

}
