package io.joshworks.eventry.index;

import java.util.Objects;

import static io.joshworks.eventry.EventId.NO_VERSION;

public class IndexEntry implements Comparable<IndexEntry> {

    public final long stream;
    public final int version;
    public final long position;
    public final long timestamp;

    private IndexEntry(long stream, int version, long position, long timestamp) {
        if (version <= NO_VERSION) {
            throw new IllegalArgumentException("Version must be at least zero");
        }
        this.stream = stream;
        this.version = version;
        this.position = position;
        this.timestamp = timestamp;
    }

    public static IndexEntry of(long stream, int version, long position, long timestamp) {
        return new IndexEntry(stream, version, position, timestamp);
    }


    @Override
    public int compareTo(IndexEntry other) {
        int keyCmp = Long.compare(stream, other.stream);
        if (keyCmp == 0) {
            keyCmp = version - other.version;
            if (keyCmp != 0)
                return keyCmp;
        }
        return keyCmp;
    }

    public boolean isDeletion() {
        return position < 0;
    }

    public boolean isTruncation() {
        return version < 0;
    }

    @Override
    public String toString() {
        return "IndexEntry{" + "stream=" + stream +
                ", version=" + version +
                ", position=" + position +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexEntry that = (IndexEntry) o;
        return stream == that.stream &&
                version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stream, version);
    }
}
