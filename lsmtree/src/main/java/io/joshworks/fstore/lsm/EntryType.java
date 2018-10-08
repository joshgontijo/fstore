package io.joshworks.fstore.lsm;

public enum EntryType {
    MEM_FLUSHED(0),
    DELETE(1),
    ADD(2);

    public final int code;

    EntryType(int code) {
        this.code = code;
    }
}
