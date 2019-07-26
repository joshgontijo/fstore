package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;

public class SSTableCompactor<K extends Comparable<K>, V> extends UniqueMergeCombiner<Entry<K, V>> {

    private final long maxAge;

    public SSTableCompactor(long maxAge) {
        this.maxAge = maxAge;
    }

    @Override
    public boolean filter(Entry<K, V> entry) {
        return entry.readable(maxAge);
    }
}
