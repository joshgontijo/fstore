package io.joshworks.eventry.index.disk.test;

public class IndexKey implements Comparable<IndexKey> {

    public static final int BYTES = Long.BYTES + Integer.BYTES;

    public final long stream;
    public final int version;

    public IndexKey(long stream, int version) {
        this.stream = stream;
        this.version = version;
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
}
