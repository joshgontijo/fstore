package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
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

    @SafeVarargs
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

    public static <T> LogIterator<T> limiting(LogIterator<T> iterator, int limit) {
        return new LimitIterator<>(iterator, limit);
    }

    public static <T> LogIterator<T> skipping(LogIterator<T> iterator, int skips) {
        return new SkippingIterator<>(iterator, skips);
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

    public static <T> Stream<T> closeableStream(LogIterator<T> iterator) {
        return closeableStream(iterator, Spliterator.ORDERED, false);
    }

    public static <T> Stream<T> closeableStream(LogIterator<T> iterator, int characteristics, boolean parallel) {
        Stream<T> delegate = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, characteristics), parallel);
        return delegate.onClose(() -> IOUtils.closeQuietly(iterator));
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

        private final Iterator<LogIterator<T>> it;
        private LogIterator<T> current;

        private IteratorIterator(List<LogIterator<T>> iterators) {
            this.it = iterators.iterator();
            nextIterator();
        }

        @Override
        public boolean hasNext() {
            nextIterator();
            boolean hasNext = current != null && current.hasNext();
            if (!hasNext) {
                this.close();
            }
            return hasNext;
        }

        private void nextIterator() {
            while ((current == null || !current.hasNext()) && it.hasNext()) {
                closeIterator(current);
                current = it.next();
            }
        }

        @Override
        public T next() {
            if (!hasNext()) {
                close();
                throw new NoSuchElementException();
            }

            return current.next();
        }

        @Override
        public long position() {
            return current.position();
        }

        @Override
        public void close() {
            closeIterator(current);
            while (it.hasNext()) {
                closeIterator(it.next());
            }
        }

        private void closeIterator(LogIterator<T> it) {
            if (it == null) {
                return;
            }
            try {
                it.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
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

    private static final class LimitIterator<T> implements LogIterator<T> {

        private final LogIterator<T> delegate;
        private final int limit;
        private final AtomicInteger processed = new AtomicInteger();

        private LimitIterator(LogIterator<T> delegate, int limit) {
            this.delegate = delegate;
            this.limit = limit;
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
            if (processed.get() >= limit) {
                IOUtils.closeQuietly(delegate);
                return false;
            }
            return delegate.hasNext();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                IOUtils.closeQuietly(delegate);
                throw new NoSuchElementException();
            }
            processed.incrementAndGet();
            return delegate.next();
        }
    }

    private static final class SkippingIterator<T> implements LogIterator<T> {

        private final LogIterator<T> delegate;
        private final int skips;
        private final AtomicInteger skipped = new AtomicInteger();

        private SkippingIterator(LogIterator<T> delegate, int skips) {
            this.delegate = delegate;
            this.skips = skips;
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
            if (skipped.get() < skips) {
                skip();
            }
            return delegate.hasNext();
        }

        @Override
        public T next() {
            if (skipped.get() < skips) {
                skip();
            }
            return delegate.next();
        }

        private void skip() {
            while (skipped.get() < skips && delegate.hasNext()) {
                delegate.next();
                skipped.incrementAndGet();
            }
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
}