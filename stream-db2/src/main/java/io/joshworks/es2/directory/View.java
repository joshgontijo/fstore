package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.iterators.CloseableIterator;
import io.joshworks.fstore.core.iterators.Iterators;

import java.io.Closeable;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class View<T extends SegmentFile> implements Iterable<T>, Closeable {

    private final AtomicInteger refs = new AtomicInteger(1);
    final TreeSet<T> segments = new TreeSet<>();

    public View() {
    }

    public View(Collection<T> segments) {
        this.segments.addAll(segments);
    }

    View<T> acquire() {
        int refCount = refs.getAndIncrement();
        if (refCount < 0) {
            refs.set(Integer.MIN_VALUE);
            throw new RuntimeException("Invalid ref count");
        }
        return this;
    }

    public T head() {
        return segments.first();
    }

    public boolean isEmpty() {
        return segments.isEmpty();
    }

    public int size() {
        return segments.size();
    }

    public View<T> apply(Consumer<TreeSet<T>> fn) {
        var copy = new TreeSet<>(segments);
        fn.accept(copy);
        return new View<>(copy);
    }

    public View<T> add(T segment) {
        var view = new View<T>(segments);
        view.segments.add(segment);
        return view;
    }

    public View<T> remove(T segment) {
        var view = new View<T>(segments);
        view.segments.add(segment);
        return view;
    }

    @Override
    public CloseableIterator<T> iterator() {
        refs.incrementAndGet();
        return Iterators.closeableIterator(segments.iterator(), this::close);
    }

    public CloseableIterator<T> reverse() {
        refs.incrementAndGet();
        return Iterators.closeableIterator(segments.descendingIterator(), this::close);
    }

    public Stream<T> stream() {
        return Iterators.stream(iterator());
    }

    @Override
    public void close() { //must not be closed twice for a given acquire
        int count = refs.decrementAndGet();
        if(count < 0) {
            throw new RuntimeException("Invalid view ref count");
        }
        if (count == 0) {
            //cleanup
        }
    }

}
