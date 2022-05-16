package io.joshworks.es2.directory;

import io.joshworks.fstore.core.iterators.Iterators;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.joshworks.fstore.core.iterators.Iterators.reversed;

public class View<T extends SegmentFile> implements Iterable<T>, Closeable {

    private final long generation;
    private final AtomicInteger refCount = new AtomicInteger(1);
    private final List<T> segments = new ArrayList<>();
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
        this.segments.sort(T::compareTo);
    }

    View<T> acquire() {
        refCount.incrementAndGet();
        return this;
    }

    private void validateReferenceCount() {
        if (refCount.get() <= 0) {
            throw new RuntimeException("View reference is invalid");
        }
    }

    private View<T> copy() {
        validateReferenceCount();
        return new View<>(segments, generation + 1);
    }

    public T head() {
        validateReferenceCount();
        return segments.get(0);
    }

    public T tail() {
        validateReferenceCount();
        return segments.get(segments.size() - 1);
    }

    public boolean isEmpty() {
        return segments.isEmpty();
    }

    public int size() {
        return segments.size();
    }

    public T get(int i) {
        return segments.get(i);
    }

    public long generation() {
        return generation;
    }

    View<T> add(T segment) {
        var view = copy();
        view.segments.add(segment);
        view.segments.sort(T::compareTo);
        return view;
    }

    View<T> deleteAll() {
        var view = new View<T>();
        markedForDeletion.addAll(segments);
        return view;
    }

    View<T> delete(Collection<T> segments) {
        var newView = copy();
        var it = newView.segments.iterator();
        while (it.hasNext()) {
            T item = it.next();
            if (segments.contains(item)) {
                it.remove();
                markedForDeletion.add(item);
            }
        }
        newView.segments.sort(T::compareTo);
        return newView;
    }

    View<T> replace(Collection<T> segments, T replacement) {
        var newView = delete(segments);
        newView.segments.add(replacement);
        newView.segments.sort(T::compareTo);
        return newView;
    }

    @Override
    public Iterator<T> iterator() {
        validateReferenceCount();
        return segments.iterator();
    }

    public Iterator<T> reverse() {
        validateReferenceCount();
        return reversed(segments);
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
