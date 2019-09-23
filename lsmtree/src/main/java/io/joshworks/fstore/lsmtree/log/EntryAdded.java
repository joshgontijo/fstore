package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

public class EntryAdded<K, V> extends LogRecord {

    public final K key;
    public final V value;

    public EntryAdded(long timestamp, K key, V value) {
        super(EntryType.ADD, timestamp);
        this.key = key;
        this.value = value;
    }
}
