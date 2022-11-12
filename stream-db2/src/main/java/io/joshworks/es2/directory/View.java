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
import java.util.stream.Collectors;
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
        int refCount = this.refCount.getAndUpdate(v -> v <= 0 ? v : v + 1);
        return refCount <= 0 ? null : this;
    }

    private View<T> copy() {
        return new View<>(segments, generation + 1);
    }

    public T head() {
        return segments.get(0);
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
        var segmentNames = segments.stream().map(SegmentFile::name).collect(Collectors.toSet());
        var it = newView.segments.iterator();
        while (it.hasNext()) {
            T item = it.next();
            if (segmentNames.contains(item.name())) {
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
        return segments.iterator();
    }

    public Iterator<T> reverse() {
        return reversed(segments);
    }

    public Stream<T> stream() {
        return Iterators.stream(iterator());
    }

    //must not be closed twice
    @Override
    public void close() { //must not be closed twice for a given acquire
        int refs = refCount.decrementAndGet();
        System.out.println("REFS: " + refs);
        if (refs == 0) {
            for (T segment : markedForDeletion) {
                segment.delete();
            }
        }
    }
}
