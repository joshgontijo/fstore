package io.joshworks.eventry.index;


import io.joshworks.fstore.index.Range;

import java.util.Objects;

public class IndexKey implements Comparable<IndexKey> {

    public static final int BYTES = Long.BYTES + Integer.BYTES;
    public static final int START_VERSION = 0;
    private static final int MAX_VERSION = Integer.MAX_VALUE;

    public final long stream;
    public final int version;

    IndexKey(long stream, int version) {
        this.stream = stream;
        this.version = version;
    }

    public static IndexKey event(long stream, int version) {
        return new IndexKey(stream, version);
    }

    public static Range<IndexKey> allOf(long stream) {
        return rangeOf(stream, START_VERSION, MAX_VERSION);
    }

    public static Range<IndexKey> rangeOf(long stream, int startInclusive, int endExclusive) {
        return Range.of(new IndexKey(stream, startInclusive), new IndexKey(stream, endExclusive));
    }

    @Override
    public int compareTo(IndexKey other) {
        int keyCmp = Long.compare(stream, other.stream);
        if (keyCmp != 0) {
            return keyCmp;
        }
        return version - other.version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexKey indexKey = (IndexKey) o;
        return stream == indexKey.stream &&
                version == indexKey.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stream, version);
    }

    @Override
    public String toString() {
        return "IndexKey{" + "stream=" + stream +
                ", version=" + version +
                '}';
    }
}
