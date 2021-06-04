package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.iterators.CloseableIterator;
import io.joshworks.fstore.core.iterators.Iterators;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
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

    private final long generation;
    private final TreeSet<SegmentItem<T>> segments = new TreeSet<>();

    View() {
        this(Collections.emptySet());
    }

    View(Collection<T> items) {
        this(items.stream()
                        .map(SegmentItem::new)
                        .collect(Collectors.toList()),
                0);
    }

    //copy constructor
    private View(Collection<SegmentItem<T>> segments, long generation) {
        this.generation = generation;
        this.segments.addAll(segments);
    }

    View<T> acquire() {
        incrementRefs();
        return this;
    }

    View<T> copy() {
        incrementRefs();
        return new View<>(segments, generation + 1);
    }

    T head() {
        return segments.first().segment;
    }

    boolean isEmpty() {
        return segments.isEmpty();
    }

    int size() {
        return segments.size();
    }

    View<T> apply(Consumer<View<T>> fn) {
        var copy = copy();
        fn.accept(copy);
        return copy;
    }

    View<T> add(T segment) {
        var view = copy();
        view.segments.add(new SegmentItem<>(segment));
        return view;
    }

    View<T> delete(T segment) {
        var view = copy();
        for (SegmentItem<T> item : view.segments) {
            if (item.segment.equals(segment)) {
                item.delete();
            }
        }
        return view;
    }

    private void incrementRefs() {
        for (SegmentItem<T> segment : segments) {
            if (segment.refCount.incrementAndGet() == 1) {
                throw new RuntimeException("Acquiring released segment");
            }
        }
    }

    @Override
    public CloseableIterator<T> iterator() {
        return mapping(closeableIterator(segments.iterator(), this::close), si -> si.segment);
    }

    public CloseableIterator<T> reverse() {
        return mapping(closeableIterator(segments.descendingIterator(), this::close), si -> si.segment);
    }

    public Stream<T> stream() {
        return Iterators.stream(iterator());
    }

    @Override
    public void close() { //must not be closed twice for a given acquire
        for (SegmentItem<T> item : segments) {
            int refs = item.refCount.decrementAndGet();
            if (refs == 0) {
                if (item.markForDeletion.get()) {
                    item.segment.delete();
                    System.out.println("Deleting " + item.segment.name());
                }
                item.segment.close();
            }
        }
    }

    static class SegmentItem<T extends SegmentFile> implements Comparable<SegmentItem<T>> {
        private final AtomicBoolean markForDeletion = new AtomicBoolean();
        private final AtomicInteger refCount = new AtomicInteger(1);
        public final T segment;

        SegmentItem(T segment) {
            this.segment = segment;
        }

        private void delete() {
            markForDeletion.set(true);
            if (refCount.decrementAndGet() == 0) {
                segment.delete();
            }
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
