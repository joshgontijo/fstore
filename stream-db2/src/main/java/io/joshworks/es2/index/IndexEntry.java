package io.joshworks.es2.index;

/**
 * STREAM (8 BYTES)
 * VERSION (4 BYTES)
 * RECORD_SIZE (4 BYTES)
 * ENTRIES (4 BYTES)
 * ADDRESS (8 BYTES)
 */
public record IndexEntry(long stream, int version, int recordSize, int entries, long logAddress) {

    public static int BYTES = Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES + Long.BYTES;

    public static int compare(IndexEntry entry, long stream, int version) {
        return IndexKey.compare(entry.stream, entry.version, stream, version);
    }

    public static int compare(IndexEntry e1, IndexEntry e2) {
        return IndexKey.compare(e1.stream, e1.version, e2.stream, e2.version);
    }
}
