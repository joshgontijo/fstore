package io.joshworks.eventry.index.disk.test;


import io.joshworks.fstore.index.Range;

public class IndexKey implements Comparable<IndexKey> {

    public static final int BYTES = Long.BYTES + Integer.BYTES;

    public final long stream;
    public final int version;

    public IndexKey(long stream, int version) {
        this.stream = stream;
        this.version = version;
    }

    public static Range<IndexKey> allOf(long stream) {
        return rangeOf(stream, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public static Range<IndexKey> rangeOf(long stream, int startInclusive, int endExclusive) {
        return Range.of(new IndexKey(stream, startInclusive), new IndexKey(stream, endExclusive));
    }

    @Override
    public int compareTo(IndexKey other) {
        int keyCmp = Long.compare(stream, other.stream);
        if (keyCmp == 0) {
            keyCmp = version - other.version;
            if (keyCmp != 0)
                return keyCmp;
        }
        return keyCmp;
    }

    @Override
    public String toString() {
        return "IndexKey{" + "stream=" + stream +
                ", version=" + version +
                '}';
    }
}
