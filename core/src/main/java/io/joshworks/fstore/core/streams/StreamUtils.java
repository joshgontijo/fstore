package io.joshworks.fstore.core.streams;

import io.joshworks.fstore.core.iterators.CloseableIterator;

import java.util.ArrayList;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class StreamUtils {
    private StreamUtils() {

    }

    public static <E extends Comparable<E>, T extends CloseableIterator<E>> Collector<T, ArrayList<T>, Stream<E>> mergeSorting() {
        return new MergeSortCollector<>();
    }

}
