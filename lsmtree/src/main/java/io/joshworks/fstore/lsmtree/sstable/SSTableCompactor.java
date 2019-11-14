


package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.appender.compaction.combiner.MergeCombiner;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.Iterator;
import java.util.List;

public class SSTableCompactor<K extends Comparable<K>, V> extends MergeCombiner<Entry<K, V>> {

    private final long maxAge;

    public SSTableCompactor(long maxAge) {
        this.maxAge = maxAge;
    }

    @Override
    public void mergeItems(List<PeekingIterator<Entry<K, V>>> items, Log<Entry<K, V>> output) {
        Entry<K, V> lastInserted = null;
        while (!items.isEmpty()) {

            PeekingIterator<Entry<K, V>> smaller = null;
            Iterator<PeekingIterator<Entry<K, V>>> itit = items.iterator();
            while (itit.hasNext()) {
                PeekingIterator<Entry<K, V>> curr = itit.next();
                if (!curr.hasNext()) {
                    itit.remove();
                    IOUtils.closeQuietly(curr);
                    continue;
                }
                if (smaller == null) {
                    smaller = curr;
                    continue;
                }
                Entry<K, V> smallerItem = smaller.peek();
                Entry<K, V> currItem = curr.peek();
                int c = smallerItem.compareTo(currItem);
                if (c == 0) { //duplicate
                    smaller.next(); //remove from oldest entry
                }
                if (c >= 0) {
                    smaller = curr;
                }
            }
            if (smaller != null) {
                Entry<K, V> next = smaller.next();
                if (lastInserted != null && next.compareTo(lastInserted) <= 0) {
                    throw new IllegalStateException("Output segment is not in sorted order");
                }
                lastInserted = next;
                output.append(next);
            }
        }
    }
}
