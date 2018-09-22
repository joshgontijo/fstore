package io.joshworks.fstore.log;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Iterators {

    private Iterators() {

    }

    public static <T> LogIterator<T> of(Collection<T> original) {
        return new ListLogIterator<>(original);
    }

    public static <T> LogIterator<T> reversed(List<T> original) {
        return new ReversedIterator<>(original);
    }

    public static <T> LogIterator<T> concat(List<LogIterator<T>> original) {
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

    public static <T> Stream<T> stream(LogIterator<T> iterator) {
        return stream(iterator, Spliterator.ORDERED, false);
    }

    public static <T> Stream<T> stream(LogIterator<T> iterator, int characteristics, boolean parallel) {
        Stream<T> delegate = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, characteristics), parallel);
        return new CloseableStream<>(iterator, delegate);
    }

    private static class EmptyIterator<T> implements LogIterator<T> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public void close() {

        }
    }

    private static class ListLogIterator<T> implements LogIterator<T> {

        private final Iterator<T> source;
        private int position;

        private ListLogIterator(Collection<T> source) {
            this.source = source.iterator();
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void close() {
            //do nothing
        }

        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public T next() {
            T next = source.next();
            if (next == null) {
                throw new NoSuchElementException();
            }
            position++;
            return next;
        }
    }

    private static class ReversedIterator<T> implements LogIterator<T> {
        final ListIterator<T> i;

        private ReversedIterator(List<T> original) {
            this.i = original.listIterator(original.size());
        }

        public boolean hasNext() {
            return i.hasPrevious();
        }

        public T next() {
            return i.previous();
        }

        public void remove() {
            i.remove();
        }

        @Override
        public long position() {
            //do nothing
            return -1;
        }

        @Override
        public void close() {
            //do nothing
        }
    }

    private static class IteratorIterator<T> implements LogIterator<T> {

        private final List<LogIterator<T>> is;
        private int current;

        private IteratorIterator(List<LogIterator<T>> iterators) {
            this.is = iterators;
            this.current = 0;
        }

        @Override
        public boolean hasNext() {
            while (current < is.size() && !is.get(current).hasNext())
                current++;

            boolean hasNext = current < is.size();
            if (!hasNext) {
                this.close();
            }
            return hasNext;
        }

        @Override
        public T next() {
            while (current < is.size() && !is.get(current).hasNext())
                current++;

            if (current >= is.size()) {
                close();
                throw new NoSuchElementException();
            }

            return is.get(current).next();
        }

        @Override
        public long position() {
            return is.get(current).position();
        }

        @Override
        public void close() {
            try {
                for (LogIterator<T> iterator : is) {
                    iterator.close();
                }
            } catch (IOException e) {
                throw RuntimeIOException.of(e);
            }

        }
    }

    public static class PeekingIterator<E> implements LogIterator<E> {

        private final LogIterator<? extends E> iterator;
        private boolean hasPeeked;
        private E peekedElement;

        public PeekingIterator(LogIterator<? extends E> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return hasPeeked || iterator.hasNext();
        }

        @Override
        public E next() {
            if (!hasPeeked) {
                return iterator.next();
            }
            E result = peekedElement;
            peekedElement = null;
            hasPeeked = false;
            return result;
        }

        @Override
        public void remove() {
            if (!hasPeeked) {
                throw new IllegalStateException("Can't remove after you've peeked at next");
            }
            iterator.remove();
        }

        public E peek() {
            if (!hasPeeked) {
                peekedElement = iterator.next();
                hasPeeked = true;
            }
            return peekedElement;
        }

        @Override
        public long position() {
            return iterator.position();
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }
    }

    private static final class FilteringIterator<T> implements LogIterator<T> {

        private final LogIterator<T> delegate;
        private Predicate<? super T> predicate;
        private T entry;

        private FilteringIterator(LogIterator<T> delegate, Predicate<? super T> predicate) {
            this.delegate = delegate;
            this.predicate = predicate;
        }

        @Override
        public long position() {
            return delegate.position();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public boolean hasNext() {
            if (entry == null) {
                entry = takeWhile();
                return entry != null;
            }
            return true;
        }

        @Override
        public T next() {
            if (entry == null) {
                entry = takeWhile();
                if (entry == null) {
                    throw new NoSuchElementException();
                }
            }
            T tmp = entry;
            entry = null;
            return tmp;
        }

        private T takeWhile() {
            T match = null;
            do {
                if (delegate.hasNext()) {
                    T next = delegate.next();
                    match = predicate.test(next) ? next : null;

                }
            } while (match == null && delegate.hasNext());
            return match;
        }
    }

    private static final class BufferingIterator<T> implements LogIterator<T> {

        private final LogIterator<T> delegate;
        private final int bufferSize;
        private final Queue<T> buffer = new LinkedList<>();

        private BufferingIterator(LogIterator<T> delegate, int bufferSize) {
            this.delegate = delegate;
            this.bufferSize = bufferSize;
        }

        @Override
        public long position() {
            return delegate.position();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public boolean hasNext() {
            return !buffer.isEmpty() || buffer();
        }

        @Override
        public T next() {
            if (buffer.isEmpty() && !buffer()) {
                throw new NoSuchElementException();
            }
            return buffer.poll();
        }

        private boolean buffer() {
            int buffered = 0;
            while (buffered <= bufferSize && delegate.hasNext()) {
                buffer.add(delegate.next());
                buffered++;
            }
            return buffered > 0;
        }
    }

    private static final class MappingIterator<R, T> implements LogIterator<R> {

        private final LogIterator<T> delegate;
        private Function<T, R> mapper;

        private MappingIterator(LogIterator<T> delegate, Function<T, R> mapper) {
            this.delegate = delegate;
            this.mapper = mapper;
        }

        @Override
        public long position() {
            return delegate.position();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public R next() {
            T entry = delegate.next();
            return mapper.apply(entry);
        }

    }


    private static final class CloseableStream<T> implements Stream<T> {

        private final Stream<T> delegate;
        private final CloseableIterator<T> iterator;

        private CloseableStream(CloseableIterator<T> iterator, Stream<T> delegate) {
            this.iterator = iterator;
            this.delegate = delegate;
        }

        @Override
        public Stream<T> filter(Predicate<? super T> predicate) {
            return delegate.filter(predicate);
        }

        @Override
        public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
            return delegate.map(mapper);
        }

        @Override
        public IntStream mapToInt(ToIntFunction<? super T> mapper) {
            return delegate.mapToInt(mapper);
        }

        @Override
        public LongStream mapToLong(ToLongFunction<? super T> mapper) {
            return delegate.mapToLong(mapper);
        }

        @Override
        public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
            return delegate.mapToDouble(mapper);
        }

        @Override
        public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
            return delegate.flatMap(mapper);
        }

        @Override
        public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
            return delegate.flatMapToInt(mapper);
        }

        @Override
        public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
            return delegate.flatMapToLong(mapper);
        }

        @Override
        public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
            return delegate.flatMapToDouble(mapper);
        }

        @Override
        public Stream<T> distinct() {
            return delegate.distinct();
        }

        @Override
        public Stream<T> sorted() {
            return delegate.sorted();
        }

        @Override
        public Stream<T> sorted(Comparator<? super T> comparator) {
            return delegate.sorted(comparator);
        }

        @Override
        public Stream<T> peek(Consumer<? super T> action) {
            return delegate.peek(action);
        }

        @Override
        public Stream<T> limit(long maxSize) {
            return delegate.limit(maxSize);
        }

        @Override
        public Stream<T> skip(long n) {
            return delegate.skip(n);
        }

        @Override
        public Stream<T> takeWhile(Predicate<? super T> predicate) {
            return delegate.takeWhile(predicate);
        }

        @Override
        public Stream<T> dropWhile(Predicate<? super T> predicate) {
            return delegate.dropWhile(predicate);
        }

        @Override
        public void forEach(Consumer<? super T> action) {
            delegate.forEach(action);
        }

        @Override
        public void forEachOrdered(Consumer<? super T> action) {
            delegate.forEachOrdered(action);
        }

        @Override
        public Object[] toArray() {
            return delegate.toArray();
        }

        @Override
        public <A> A[] toArray(IntFunction<A[]> generator) {
            return delegate.toArray(generator);
        }

        @Override
        public T reduce(T identity, BinaryOperator<T> accumulator) {
            return delegate.reduce(identity, accumulator);
        }

        @Override
        public Optional<T> reduce(BinaryOperator<T> accumulator) {
            return delegate.reduce(accumulator);
        }

        @Override
        public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
            return delegate.reduce(identity, accumulator, combiner);
        }

        @Override
        public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
            return delegate.collect(supplier, accumulator, combiner);
        }

        @Override
        public <R, A> R collect(Collector<? super T, A, R> collector) {
            return delegate.collect(collector);
        }

        @Override
        public Optional<T> min(Comparator<? super T> comparator) {
            return delegate.min(comparator);
        }

        @Override
        public Optional<T> max(Comparator<? super T> comparator) {
            return delegate.max(comparator);
        }

        @Override
        public long count() {
            return delegate.count();
        }

        @Override
        public boolean anyMatch(Predicate<? super T> predicate) {
            return delegate.anyMatch(predicate);
        }

        @Override
        public boolean allMatch(Predicate<? super T> predicate) {
            return delegate.allMatch(predicate);
        }

        @Override
        public boolean noneMatch(Predicate<? super T> predicate) {
            return delegate.noneMatch(predicate);
        }

        @Override
        public Optional<T> findFirst() {
            return delegate.findFirst();
        }

        @Override
        public Optional<T> findAny() {
            return delegate.findAny();
        }

        @Override
        public Iterator<T> iterator() {
            return delegate.iterator();
        }

        @Override
        public Spliterator<T> spliterator() {
            return delegate.spliterator();
        }

        @Override
        public boolean isParallel() {
            return delegate.isParallel();
        }

        @Override
        public Stream<T> sequential() {
            return delegate.sequential();
        }

        @Override
        public Stream<T> parallel() {
            return delegate.parallel();
        }

        @Override
        public Stream<T> unordered() {
            return delegate.unordered();
        }

        @Override
        public Stream<T> onClose(Runnable closeHandler) {
            return delegate.onClose(closeHandler);
        }

        @Override
        public void close() {
            delegate.close();
            try {
                iterator.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


}