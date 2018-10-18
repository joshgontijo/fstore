package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * For <b>UNIQUE</b> and <b>SORTED</b> segments only
 * Guaranteed uniqueness only for segments that holds unique items.
 * For items that {@link Comparable#compareTo(Object)} returns equals. The last (newest) item in the list will be used.
 */
public class UniqueMergeCombiner<T extends Comparable<T>> extends MergeCombiner<T> {

    //FIXME TEST IT
    @Override
    public void mergeItems(List<Iterators.PeekingIterator<T>> items, Log<T> output) {

        TreeSet<T> set = new TreeSet<>();

        while (!items.isEmpty()) {
            //reversed guarantees that the most recent data is kept when duplicate keys are found
            Iterator<Iterators.PeekingIterator<T>> itit = Iterators.reversed(items);
            while (itit.hasNext()) {
                Iterators.PeekingIterator<T> seg = itit.next();
                if (!seg.hasNext()) {
                    itit.remove();
                    continue;
                }
                T next = seg.next();
                set.add(next);
            }

            T t = set.pollFirst();
            output.append(t);
        }
        while (!set.isEmpty()) {
            T t = set.pollFirst();
            output.append(t);
        }
    }

    /**
     * Returns true if this entry should be appended to the new segment, false otherwise
     */
    public boolean filter(T entry) {
        return true;
    }
}
