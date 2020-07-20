package io.joshworks.es.index;

public class IndexKey implements Comparable<IndexKey> {

    public final long stream;
    public final int version;

    public IndexKey(long stream, int version) {
        this.stream = stream;
        this.version = version;
    }

    public static IndexKey maxOf(long stream) {
        return new IndexKey(stream, Integer.BYTES);
    }

    @Override
    public int compareTo(IndexKey o) {
        return compare(this, o.stream, o.version);
    }

    public static int compare(IndexKey key, long stream, int version) {
        int compare = Long.compare(key.stream, stream);
        if (compare == 0) {
            return Integer.compare(key.version, version);
        }
        return compare;
    }

}
