package io.joshworks.fstore.log.segment.header;

import java.util.Objects;

public final class MergeSection {
    public final long timestamp;

    public MergeSection(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergeSection that = (MergeSection) o;
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
