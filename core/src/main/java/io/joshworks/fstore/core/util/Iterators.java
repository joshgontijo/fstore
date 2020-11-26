package io.joshworks.fstore.core.util;

import io.joshworks.fstore.core.io.IOUtils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Iterators {

    private Iterators() {

    }

    public static boolean await(Iterator<?> it, long pollMs, long maxTime) {
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

    public static <T> Iterator<T> of(T... items) {
        return Arrays.asList(items).iterator();
    }

    public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    }

}