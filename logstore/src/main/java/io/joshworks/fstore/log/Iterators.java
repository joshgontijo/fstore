package io.joshworks.fstore.log;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
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

    public static <T> LogIterator<T> flatten(List<LogIterator<T>> original) {
        return flatten(new ArrayList<>(original).iterator());
    }

    public static <T> LogIterator<T> flatten(LogIterator<T>... originals) {
        return flatten(Arrays.asList(originals).iterator());
    }

    public static <T> LogIterator<T> flatten(Iterator<LogIterator<T>> iterator) {
        return new IteratorIterator<>(iterator);
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
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    public static <T> Stream<T> stream(LogIterator<T> iterator, int characteristics, boolean parallel) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, characteristics), parallel);
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

        private final Iterator<LogIterator<T>> iterators;
        private LogIterator<T> current;

        private IteratorIterator(Iterator<LogIterator<T>> iterators) {
            this.iterators = iterators;
            this.current = this.iterators.hasNext() ? this.iterators.next() : Iterators.empty();
        }

        @Override
        public boolean hasNext() {
            while (!current.hasNext() && iterators.hasNext())
                current = iterators.next();

            boolean hasNext = current.hasNext();
            if (!hasNext) {
                this.close();
            }
            return hasNext;
        }

        @Override
        public T next() {
            while (!current.hasNext() && iterators.hasNext()) {
                closeIterator(current);
                current = iterators.next();
            }

            if (!current.hasNext()) {
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
            while (iterators.hasNext()) {
                closeIterator(iterators.next());
            }
        }

        private void closeIterator(Closeable closeable) {
            try {
                closeable.close();
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

}