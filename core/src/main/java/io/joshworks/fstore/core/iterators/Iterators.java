package io.joshworks.fstore.core.iterators;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Threads;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Iterators {

    private Iterators() {

    }

    public static boolean await(CloseableIterator<?> it, long pollMs, long maxTime) {
        pollMs = Math.min(pollMs, maxTime);
        long start = System.currentTimeMillis();
        while (!it.hasNext()) {
            if (System.currentTimeMillis() - start > maxTime) {
                return false;
            }
            Threads.sleep(pollMs);
        }
        return true;
    }

    public static <T> Iterator<T> of(T... items) {
        return Arrays.asList(items).iterator();
    }

    public static <T> CloseableIterator<T> of(Collection<T> original) {
        return new ListLogIterator<>(original);
    }

    public static <T> CloseableIterator<T> wrap(Iterator<T> original) {
        return new WrappedIterator<>(original);
    }

    public static <T> CloseableIterator<T> reversed(List<T> original) {
        return new ReversedIterator<>(original);
    }

    public static <T> CloseableIterator<T> roundRobin(Collection<? extends CloseableIterator<T>> original) {
        return new IteratorIterator<>(original);
    }

    public static <T> CloseableIterator<T> concat(Collection<? extends CloseableIterator<T>> original) {
        return new IteratorIterator<>(original);
    }

    public static <T> CloseableIterator<T> concat(CloseableIterator<T>... originals) {
        return new IteratorIterator<>(Arrays.asList(originals));
    }

    public static <T, R> CloseableIterator<R> mapping(CloseableIterator<T> iterator, Function<T, R> mapper) {
        return new MappingIterator<>(iterator, mapper);
    }

    public static <T> CloseableIterator<T> filtering(CloseableIterator<T> iterator, Predicate<? super T> predicate) {
        return new FilteringIterator<>(iterator, predicate);
    }

    public static <T> CloseableIterator<T> buffering(CloseableIterator<T> iterator, int bufferSize) {
        return new BufferingIterator<>(iterator, bufferSize);
    }

    public static <T> CloseableIterator<List<T>> batching(CloseableIterator<T> iterator, int batchSize) {
        return new BatchingIterator<>(iterator, batchSize);
    }

    /**
     * For <b>SORTED</b> iterators only.
     */
    public static <T> CloseableIterator<T> merging(Collection<? extends CloseableIterator<T>> iterators, Comparator<T> cmp) {
        return new MergeSortIterator<>(iterators.stream()
                .map(PeekingIterator::new)
                .collect(Collectors.toList()), cmp);
    }

    /**
     * For <b>SORTED</b> iterators only.
     */
    public static <T extends Comparable<T>> CloseableIterator<T> merging(Collection<? extends CloseableIterator<T>> iterators) {
        return merging(iterators, T::compareTo);
    }

    public static <T> PeekingIterator<T> peekingIterator(CloseableIterator<T> iterator) {
        return new PeekingIterator<>(iterator);
    }

    public static <T> CloseableIterator<T> closeableIterator(Iterator<T> iterator) {
        return closeableIterator(iterator, () -> {
        });
    }

    public static <T> CloseableIterator<T> closeableIterator(Iterator<T> iterator, Runnable onClose) {
        return new CloseableIterator<T>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }

            @Override
            public void close() {
                onClose.run();
            }
        };
    }

    public static <T> CloseableIterator<T> empty() {
        return new EmptyIterator<>();
    }

    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> copy = new ArrayList<>();
        while (iterator.hasNext())
            copy.add(iterator.next());
        return copy;
    }

    public static <T> Stream<T> closeableStream(CloseableIterator<T> iterator) {
        return closeableStream(iterator, Spliterator.ORDERED, false);
    }


    public static <T> Stream<T> closeableStream(CloseableIterator<T> iterator, int characteristics, boolean parallel) {
        Stream<T> delegate = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, characteristics), parallel);
        return delegate.onClose(() -> IOUtils.closeQuietly(iterator));
    }

    public static <T> Stream<T> stream(Collection<T> collection) {
        return stream(collection.iterator());
    }

    public static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

}