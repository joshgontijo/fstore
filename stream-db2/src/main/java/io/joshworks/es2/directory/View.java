package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class View<T extends SegmentFile> implements Closeable, Iterable<T> {

    private final AtomicInteger refCounter = new AtomicInteger();
    private final Set<T> markedForDeletion = new HashSet<>();

    final ConcurrentSkipListSet<T> segments = new ConcurrentSkipListSet<>();

    View() {
        refCounter.incrementAndGet();
    }

    @Override
    public Iterator<T> iterator() {
        return segments.iterator();
    }

    public Iterator<T> descendingIterator() {
        return segments.descendingIterator();
    }

    public T head() {
        return segments.first();
    }

    public T tail() {
        return segments.last();
    }

    @Override
    public void close() {
        boolean markedForDeletion = !this.markedForDeletion.isEmpty();
        int refs = refCounter.decrementAndGet();
        if (refs == 0 && markedForDeletion) {
            for (T item : this.markedForDeletion) {
                item.delete();
            }
        }
    }

    synchronized View<T> copy() {
        View<T> copy = new View<>();
        copy.segments.addAll(segments);
        copy.segments.removeAll(markedForDeletion);

    }

    void markForDeletion(T item) {
        assert refCounter.get() > 0 : "No valid reference to this view";
        markedForDeletion.add(item);
    }

    View<T> acquire() {
        refCounter.incrementAndGet();
        return this;
    }
}
