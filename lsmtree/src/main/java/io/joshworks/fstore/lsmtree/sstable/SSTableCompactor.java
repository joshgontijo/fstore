package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.lsmtree.EntryType;

import java.util.List;

public class SSTableCompactor<K extends Comparable<K>, V> extends UniqueMergeCombiner<Entry<K, V>> {

    @Override
    public void merge(List<? extends Log<Entry<K, V>>> segments, Log<Entry<K, V>> output) {
        SSTable<K, V> sstable = (SSTable<K, V>) output;
        long totalEntries = segments.stream().mapToLong(Log::entries).sum();
        sstable.newBloomFilter(totalEntries);
        super.merge(segments, output);
    }

    @Override
    public boolean filter(Entry<K, V> entry) {
        return EntryType.ADD.equals(entry.type);
    }
}
