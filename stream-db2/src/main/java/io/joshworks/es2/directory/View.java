package io.joshworks.es2.directory;

import io.joshworks.fstore.core.iterators.Iterators;
import org.slf4j.Logger;

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
    private final Logger logger;
    private final AtomicInteger refCount = new AtomicInteger(1);
    private final List<T> segments = new ArrayList<>();
    private final Set<T> markedForDeletion = new HashSet<>();

    View(Logger logger) {
        this(Collections.emptySet(), logger);
    }

    View(Collection<T> items, Logger logger) {
        this(items, 0, logger);
    }

    //copy constructor
    private View(Collection<T> segments, long generation, Logger logger) {
        this.generation = generation;
        this.logger = logger;
        this.segments.addAll(segments);
        this.segments.sort(T::compareTo);
    }

    synchronized View<T> acquire() {
        int refCount = this.refCount.getAndUpdate(v -> v <= 0 ? v : v + 1);
        return refCount <= 0 ? null : this;
    }

    private View<T> copy() {
        return new View<>(segments, generation + 1, this.logger);
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

    synchronized View<T> add(T segment) {
        var view = copy();
        view.segments.add(segment);
        view.segments.sort(T::compareTo);
        return view;
    }

    synchronized View<T> deleteAll() {
        var view = new View<T>(this.logger);
        markedForDeletion.addAll(segments);
        return view;
    }

    synchronized View<T> delete(Collection<T> segments) {
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

    synchronized View<T> replace(Collection<T> segments, T replacement) {
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
    public synchronized void close() { //must not be closed twice for a given acquire
        int refs = refCount.decrementAndGet();
        if (refs == 0) {
            logger.info("### DROPING GEN " + this.generation + " ###");
            if (this.generation == 0) {
                System.out.println();
            }
            for (T segment : markedForDeletion) {
                segment.delete();
            }
        }
    }
}
