package io.joshworks.fstore.lsmtree;

public enum EntryType {
    MEM_FLUSH_STARTED((short) 0),
    MEM_FLUSHED((short) 1),
    DELETE((short) 2),
    ADD((short) 3);

    public final short code;

    EntryType(short code) {
        this.code = code;
    }

    public static EntryType of(short code) {
        return EntryType.values()[code];
    }

}
