package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;

public class SSTableCompactor<K extends Comparable<K>, V> extends UniqueMergeCombiner<Entry<K, V>> {

    @Override
    public boolean filter(Entry<K, V> entry) {
        return !entry.deletion();
    }
}
