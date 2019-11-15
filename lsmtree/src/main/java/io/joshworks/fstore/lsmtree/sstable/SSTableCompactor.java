


package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.compaction.combiner.MergeCombiner;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;

public class SSTableCompactor<K extends Comparable<K>, V> extends MergeCombiner<Entry<K, V>> {

    private final long maxAge;

    public SSTableCompactor(long maxAge) {
        this.maxAge = maxAge;
    }

    @Override
    public void mergeItems(List<PeekingIterator<Entry<K, V>>> items, Log<Entry<K, V>> output) {
        CloseableIterator<Entry<K, V>> iterator = new SSTablesIterator<>(Direction.FORWARD, items);
        iterator = Iterators.filtering(iterator, e -> !e.expired(maxAge));
        while (iterator.hasNext()) {
            output.append(iterator.next());
        }
    }
}
