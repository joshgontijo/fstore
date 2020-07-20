package io.joshworks.es.index;

public record IndexEntry(long stream, int version, int size, long logAddress)  {

    public static int compare(IndexEntry entry, long stream, int version) {
        return IndexKey.compare(entry.stream, entry.version, stream, version);
    }

    public static int compare(IndexEntry e1, IndexEntry e2) {
        return IndexKey.compare(e1.stream, e1.version, e2.stream, e2.version);
    }
}
