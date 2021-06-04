package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.iterators.CloseableIterator;
import io.joshworks.fstore.core.iterators.Iterators;

import java.io.Closeable;
import java.util.Collection;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.joshworks.fstore.core.iterators.Iterators.closeableIterator;
import static io.joshworks.fstore.core.iterators.Iterators.mapping;

public class View<T extends SegmentFile> implements Iterable<T>, Closeable {

    private final AtomicInteger refs = new AtomicInteger(1);
    final TreeSet<SegmentItem<T>> segments = new TreeSet<>();

    private View() {
    }


    static <T extends SegmentFile> View<T> empty() {
        return new View<>();
    }

    static <T extends SegmentFile> View<T> initialize(Collection<T> segments) {
        View<T> view = new View<>();
        view.segments.addAll(segments.stream()
                .map(SegmentItem::new)
                .collect(Collectors.toList()));
        return view;
    }

    static <T extends SegmentFile> View<T> copy(Collection<SegmentItem<T>> segments) {
        var view = new View<T>();
        view.segments.addAll(segments);
        return view;
    }

    public T head() {
        return segments.first().segment;
    }

    public boolean isEmpty() {
        return segments.isEmpty();
    }

    public int size() {
        return segments.size();
    }

    public View<T> apply(Consumer<View<T>> fn) {
        var copy = View.copy(segments);
        fn.accept(copy);
        return copy;
    }

    public View<T> add(T segment) {
        var view = View.copy(segments);
        view.segments.add(new SegmentItem<>(segment));
        return view;
    }

    public View<T> delete(T segment) {
        var view = View.copy(segments);
        SegmentItem<T> item = new SegmentItem<>(segment);
        if (!view.segments.remove(item)) {
            throw new RuntimeException("Failed to delete segment " + segment.name() + ": Not in view");
        }
        for (SegmentItem<T> item : view.segments) {

        }

        return view;
    }

    @Override
    public CloseableIterator<T> iterator() {
        refs.incrementAndGet();
        return mapping(closeableIterator(segments.iterator(), this::close), si -> si.segment);
    }

    public CloseableIterator<T> reverse() {
        refs.incrementAndGet();
        return mapping(closeableIterator(segments.descendingIterator(), this::close), si -> si.segment);
    }

    public Stream<T> stream() {
        return Iterators.stream(iterator());
    }

    @Override
    public void close() { //must not be closed twice for a given acquire
        int count = refs.decrementAndGet();
        if (count < 0) {
            throw new RuntimeException("Invalid view ref count");
        }
        if (count == 0) {
            //cleanup
            System.out.println("View cleanup");
        }
    }

    static class SegmentItem<T extends SegmentFile> implements Comparable<SegmentItem<T>> {
        private final AtomicBoolean markForDeletion = new AtomicBoolean();
        public final T segment;

        SegmentItem(T segment) {
            this.segment = segment;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SegmentItem<?> that = (SegmentItem<?>) o;
            return segment.equals(that.segment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(segment);
        }

        @Override
        public int compareTo(SegmentItem<T> o) {
            return this.segment.compareTo(o.segment);
        }
    }


}
