package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.log.segment.WriteMode;

public final class OpenSection {
    public final String magic;
    public final long created;
    public final WriteMode mode;
    public final long fileSize;
    public final boolean encrypted;

    public OpenSection(String magic, long created, WriteMode mode, long fileSize, boolean encrypted) {
        this.magic = magic;
        this.created = created;
        this.mode = mode;
        this.fileSize = fileSize;
        this.encrypted = encrypted;
    }

    @Override
    public String toString() {
        return "{" +
                "magic='" + magic + '\'' +
                ", created=" + created +
                ", mode=" + mode +
                ", fileSize=" + fileSize +
                ", encrypted=" + encrypted +
                '}';
    }
}
