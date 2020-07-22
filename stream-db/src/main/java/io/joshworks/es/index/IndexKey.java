package io.joshworks.es.index;

import io.joshworks.es.StreamHasher;

public record IndexKey(long stream, int version) {

    public static int BYTES = Long.BYTES + Integer.BYTES;

    public static IndexKey maxOf(long stream) {
        return new IndexKey(stream, Integer.MAX_VALUE);
    }

    public static IndexKey of(String stream, int version) {
        return new IndexKey(StreamHasher.hash(stream), version);
    }

    public static int compare(IndexKey key, long stream, int version) {
        return compare(key.stream, key.version, stream, version);
    }

    public static int compare(IndexKey k1, IndexKey k2) {
        return compare(k1.stream, k1.version, k2.stream, k2.version);
    }

    public static int compare(long stream1, int version1, long stream2, int version2) {
        int compare = Long.compare(stream1, stream2);
        if (compare == 0) {
            return Integer.compare(version1, version2);
        }
        return compare;
    }

    public static String toString(long stream, int version) {
        return stream + "@" + version;
    }

    @Override
    public String toString() {
        return toString(stream, version);
    }
}
