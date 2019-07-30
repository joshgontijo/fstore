package io.joshworks.fstore.lsmtree;

public class EntryValue<V> {

    public final V value;
    public final long timestamp;

    EntryValue(V value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
}
