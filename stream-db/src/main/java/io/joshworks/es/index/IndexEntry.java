package io.joshworks.es.index;

public class IndexEntry extends IndexKey {

    public final int size;
    public final long logAddress;

    public IndexEntry(long stream, int version, int size, long logAddress) {
        super(stream, version);
        this.size = size;
        this.logAddress = logAddress;
    }
}
