package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Guaranteed uniqueness only for segments that holds unique items.
 * For items that {@link Comparable#compareTo(Object)} returns equals. The last (newest) item in the list will be used.
 */
public class UniqueMergeCombiner<T extends Comparable<T>> extends MergeCombiner<T> {


    @Override
    public void mergeItems(List<Iterators.PeekingIterator<T>> items, Log<T> output) {

        TreeSet<T> set = new TreeSet<>();

        while (!items.isEmpty()) {
            Iterator<Iterators.PeekingIterator<T>> itit = items.iterator();
            while (itit.hasNext()) {
                Iterators.PeekingIterator<T> seg = itit.next();
                set.add(seg.next());
                if (!seg.hasNext()) {
                    itit.remove();
                }
            }

            T t = set.pollFirst();
            System.out.println("-> " + t);
            output.append(t);
        }
        while (!set.isEmpty()) {
            T t = set.pollFirst();
            System.out.println("-> " + t);
            output.append(t);
        }
    }

    /**
     * Returns true if this entry should be appended to the new segment, false otherwise
     */
    public boolean filter(T entry) {
        return true;
    }

    private static class ComparableIterator<T extends Comparable<T>> extends Iterators.PeekingIterator<T> implements Comparable<ComparableIterator<T>> {

        public ComparableIterator(LogIterator<T> it) {
            super(it);
        }

        @Override
        public int compareTo(ComparableIterator<T> o) {
            int i;
            do {
                T thisItem = this.hasNext() ? this.peek() : null;
                T otherItem = o.hasNext() ? o.peek() : null;

                if (thisItem == null) {
                    return 1;
                }
                if (otherItem == null) {
                    return 1;
                }

                i = thisItem.compareTo(otherItem);
                if (i == 0) { //if elements are equal, remove the first (oldest) and keep newest
                    this.next();
                }
            } while (i == 0);

            return i;
        }

    }


    // @Override
    //    public void mergeItems(List<Iterators.PeekingIterator<T>> items, Log<T> output) {
    //
    //        TreeSet<Iterators.PeekingIterator<T>> set = new TreeSet<>((o1, o2) -> {
    //            int i;
    //            if(o1 == o2) {
    //                return 0;
    //            }
    //            do {
    //                T i1 = o1.peek();
    //                T i2 = o2.peek();
    //                i = i1.compareTo(i2);
    //                if(i == 0) {
    //                    o1.next();
    //                }
    //            }while(i == 0);
    //            return i;
    //        });
    //
    //        set.addAll(items);
    //
    //        while (!set.isEmpty()) {
    //            Iterators.PeekingIterator<T> it = set.pollFirst();
    //            if (it.hasNext()) {
    //                T next = it.next();
    //                if (filter(next)) {
    //                    System.out.println("-> " + next);
    //                    output.append(next);
    //                }
    //                if (it.hasNext()) {
    //                    set.add(it);
    //                }
    //            }
    //        }
    //    }

}
