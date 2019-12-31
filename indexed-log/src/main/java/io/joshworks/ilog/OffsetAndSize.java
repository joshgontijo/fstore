package io.joshworks.ilog;

public class OffsetAndSize<K> {

    public final K startKey;
    public final long offset;
    public final int slotSize;

    public OffsetAndSize(K startKey, long offset, int slotSize) {
        this.startKey = startKey;
        this.offset = offset;
        this.slotSize = slotSize;
    }
}
