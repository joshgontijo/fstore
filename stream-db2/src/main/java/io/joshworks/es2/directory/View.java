//package io.joshworks.es2.directory;
//
//import io.joshworks.es2.SegmentFile;
//
//import java.io.Closeable;
//import java.io.File;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.NavigableSet;
//import java.util.Set;
//import java.util.SortedSet;
//import java.util.concurrent.locks.Lock;
//
//public class View<T extends SegmentFile> implements Closeable, Iterable<T> {
//
//   private final Lock lock;
//    private final NavigableSet<T> segments;
//
//    public View(Lock lock, NavigableSet<T> segments) {
//        this.lock = lock;
//        this.segments = segments;
//    }
//
//
//    @Override
//    public Iterator<T> iterator() {
//        return segments.iterator();
//    }
//
//    public Iterator<T> descendingIterator() {
//        return segments.descendingIterator();
//    }
//
//    public T head() {
//        return segments.first();
//    }
//
//    public T tail() {
//        return segments.last();
//    }
//
//    boolean isEmpty() {
//        return segments.isEmpty();
//    }
//
//    @Override
//    public void close() {
//        lock.unlock();
//    }
//
//    synchronized View<T> append(T item) {
//        View<T> copy = new View<>(segments, segments);
//        copy.segments.add(item);
//        copy.merging.addAll(merging);
//        return copy;
//    }
//
//    synchronized MergeHandle<T> createMerge(int level, int minItems, int maxItems, File result) {
//        List<T> items = new ArrayList<>();
//        for (T segment : segments) {
//            if (items.size() >= maxItems) {
//                break;
//            }
//            if (DirectoryUtils.level(segment) == level) {
//                items.add(segment);
//            }
//        }
//        if (items.size() < minItems) {
//            return null;
//        }
//        merging.addAll(items);
//        return new MergeHandle<>(result, items, this.acquire());
//    }
//
//    synchronized View<T> merge(MergeHandle<T> mergeItem) {
//        if (!segments.containsAll(mergeItem.sources)) {
//            throw new IllegalStateException("Merge segment mismatch");
//        }
//
//        for (T segment : segments) {
//            markForDeletion(segment);
//        }
//
//        View<T> copy = new View<>(lock, segments);
//        copy.segments.addAll(segments);
//        copy.segments.removeAll(mergeItem.sources);
//        copy.segments.add(mergeItem.replacement);
//        return copy;
//    }
//
//    private void markForDeletion(T item) {
//        assert refCounter.get() > 0 : "No valid reference to this view";
//        markedForDeletion.add(item);
//    }
//
//    View<T> acquire() {
//        refCounter.incrementAndGet();
//        return this;
//    }
//}
