package io.joshworks.fstore.index;


import io.joshworks.fstore.lsmtree.Range;

import java.util.Objects;

import static io.joshworks.fstore.es.shared.EventId.MAX_VERSION;
import static io.joshworks.fstore.es.shared.EventId.START_VERSION;

public class IndexKey implements Comparable<IndexKey> {

    public static final int BYTES = Long.BYTES + Integer.BYTES;

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
