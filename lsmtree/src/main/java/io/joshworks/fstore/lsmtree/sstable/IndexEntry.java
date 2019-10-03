package io.joshworks.fstore.lsmtree.sstable;

public class IndexEntry<K extends Comparable<K>> extends Entry<K, Long> {

    IndexEntry(long timestamp, K key, Long value) {
        super(timestamp, key, value);
    }

    @Override
    public boolean deletion() {
        return value < 0;
    }
}
