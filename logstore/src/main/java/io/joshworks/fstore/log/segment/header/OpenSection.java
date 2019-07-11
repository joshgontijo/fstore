package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.log.segment.WriteMode;

import java.util.Objects;

public final class OpenSection {
    public final long created;
    public final WriteMode mode;
    public final long fileSize;
    public final long dataSize;
    public final boolean encrypted;

    public OpenSection(long created, WriteMode mode, long fileSize, long dataSize, boolean encrypted) {
        this.created = created;
        this.mode = mode;
        this.fileSize = fileSize;
        this.dataSize = dataSize;
        this.encrypted = encrypted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenSection that = (OpenSection) o;
        return created == that.created &&
                fileSize == that.fileSize &&
                dataSize == that.dataSize &&
                encrypted == that.encrypted &&
                mode == that.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(created, mode, fileSize, dataSize, encrypted);
    }

    @Override
    public String toString() {
        return "{" + "created=" + created +
                ", mode=" + mode +
                ", fileSize=" + fileSize +
                ", dataSize=" + dataSize +
                ", encrypted=" + encrypted +
                '}';
    }
}
