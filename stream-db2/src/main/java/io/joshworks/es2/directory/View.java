package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.iterators.Iterators;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class View<T extends SegmentFile> implements Iterable<T>, Closeable {

    private final long generation;
    private final AtomicInteger refCount = new AtomicInteger(1);
    private final TreeSet<T> segments = new TreeSet<>();
    private final Set<T> markedForDeletion = new HashSet<>();

    View() {
        this(Collections.emptySet());
    }

    View(Collection<T> items) {
        this(items, 0);
    }

    //copy constructor
    private View(Collection<T> segments, long generation) {
        this.generation = generation;
        this.segments.addAll(segments);
    }

    View<T> acquire() {
        refCount.incrementAndGet();
        return this;
    }

    View<T> copy() {
        return new View<>(segments, generation + 1);
    }

    T head() {
        return segments.first();
    }

    boolean isEmpty() {
        return segments.isEmpty();
    }

    int size() {
        return segments.size();
    }

    View<T> add(T segment) {
        var view = copy();
        view.segments.add(segment);
        return view;
    }

    View<T> deleteAll() {
        var view = new View<T>();
        markedForDeletion.addAll(segments);
        return view;
    }

    View<T> replace(Collection<T> segments, T replacement) {
        var newView = copy();
        var it = newView.segments.iterator();
        while (it.hasNext()) {
            T item = it.next();
            if (segments.contains(item)) {
                it.remove();
                markedForDeletion.add(item);
            }
        }
        newView.segments.add(replacement);
        return newView;
    }

    @Override
    public Iterator<T> iterator() {
        return segments.iterator();
    }

    public Iterator<T> reverse() {
        return segments.descendingIterator();
    }

    public Stream<T> stream() {
        return Iterators.stream(iterator());
    }

    @Override
    public void close() { //must not be closed twice for a given acquire
        int refs = refCount.decrementAndGet();
        if (refs == 0 && !markedForDeletion.isEmpty()) {
            for (T segment : markedForDeletion) {
                segment.delete();
            }
        }
    }
}
