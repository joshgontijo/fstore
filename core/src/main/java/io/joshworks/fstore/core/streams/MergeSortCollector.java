package io.joshworks.fstore.core.streams;

import io.joshworks.fstore.core.iterators.CloseableIterator;
import io.joshworks.fstore.core.iterators.Iterators;

import java.util.ArrayList;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

class MergeSortCollector<T extends CloseableIterator<E>, E extends Comparable<E>> implements Collector<T, ArrayList<T>, Stream<E>> {
    @Override
    public Supplier<ArrayList<T>> supplier() {
        return ArrayList::new;
    }

    @Override
    public BiConsumer<ArrayList<T>, T> accumulator() {
        return ArrayList::add;
    }

    @Override
    public BinaryOperator<ArrayList<T>> combiner() {
        throw new RuntimeException("Cannot run in parallel");
    }

    @Override
    public Function<ArrayList<T>, Stream<E>> finisher() {
        return i -> Iterators.stream(Iterators.merging(i));
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.IDENTITY_FINISH);
    }
}
