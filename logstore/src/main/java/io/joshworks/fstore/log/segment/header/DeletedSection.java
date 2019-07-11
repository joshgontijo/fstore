package io.joshworks.fstore.log.segment.header;

import java.util.Objects;

public final class DeletedSection {
    public final long timestamp;

    public DeletedSection(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeletedSection that = (DeletedSection) o;
        return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp);
    }

    @Override
    public String toString() {
        return "{" +
                "timestamp=" + timestamp +
                '}';
    }
}
