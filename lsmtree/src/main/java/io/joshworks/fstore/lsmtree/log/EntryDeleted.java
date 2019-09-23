package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

public class EntryDeleted<K> extends LogRecord {

    public final K key;

    public EntryDeleted(long timestamp, K key) {
        super(EntryType.DELETE, timestamp);
        this.key = key;
    }
}
