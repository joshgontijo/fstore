package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.LogIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Iterators {

    private Iterators() {

    }

    public static <T> T await(LogIterator<T> it, long pollMs) {
        while(!it.hasNext()) {
            Threads.sleep(pollMs);
        }
        return it.next();
    }

    public static boolean await(LogIterator<?> it, long pollMs, long maxTime) {
        pollMs = Math.min(pollMs, maxTime);
        long start = System.currentTimeMillis();
        while(!it.hasNext()) {
            if(System.currentTimeMillis() - start > maxTime) {
                return false;
            }
            Threads.sleep(pollMs);
        }
        return true;
    }

    public static <T> LogIterator<T> of(Collection<T> original) {
        return new ListLogIterator<>(original);
    }

    public static <T> LogIterator<T> reversed(List<T> original) {
        return new ReversedIterator<>(original);
    }

    public static <T> LogIterator<T> concat(Collection<? extends LogIterator<T>> original) {
        return new IteratorIterator<>(original);
    }

    public static <T> LogIterator<T> concat(LogIterator<T>... originals) {
        return new IteratorIterator<>(Arrays.asList(originals));
    }

    public static <T, R> LogIterator<R> mapping(LogIterator<T> iterator, Function<T, R> mapper) {
        return new MappingIterator<>(iterator, mapper);
    }

    public static <T> LogIterator<T> filtering(LogIterator<T> iterator, Predicate<? super T> predicate) {
        return new FilteringIterator<>(iterator, predicate);
    }

    public static <T> LogIterator<T> buffering(LogIterator<T> iterator, int bufferSize) {
        return new BufferingIterator<>(iterator, bufferSize);
    }

    public static <T> LogIterator<List<T>> batching(LogIterator<T> iterator, int batchSize) {
        return new BatchingIterator<>(iterator, batchSize);
    }

    //not streaming in the sense of java.util.Stream
    //it means it will streamline a batched iterator
    //this is the opposite of batching
    public static <T> LogIterator<T> streaming(LogIterator<Collection<T>> iterator) {
        return new StreamingIterator<>(iterator);
    }

    public static <T> LogIterator<T> limiting(LogIterator<T> iterator, int limit) {
        return new LimitIterator<>(iterator, limit);
    }

    public static <T> LogIterator<T> skipping(LogIterator<T> iterator, int skips) {
        return new SkippingIterator<>(iterator, skips);
    }

    /**
     * For <b>SORTED</b> iterators only.
     */
    public static <T, C extends Comparable<C>> LogIterator<T> ordered(Collection<? extends LogIterator<T>> iterators, Function<T, C> mapper) {
        return new OrderedIterator<>(iterators, mapper);
    }

    /**
     * For <b>UNIQUE</b> and <b>SORTED</b> iterators only.
     */
    public static <T extends Comparable<T>> LogIterator<T> ordered(Collection<? extends LogIterator<T>> iterators) {
        return new OrderedIterator<>(iterators, t -> t);
    }

    public static <T> LogIterator<T> merging(Collection<T> iterators, int skips) {
        throw new UnsupportedOperationException("TODO");
    }

    public static <T> PeekingIterator<T> peekingIterator(LogIterator<T> iterator) {
        return new PeekingIterator<>(iterator);
    }

    public static <T> LogIterator<T> empty() {
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