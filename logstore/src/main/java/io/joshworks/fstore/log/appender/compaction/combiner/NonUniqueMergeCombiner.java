package io.joshworks.fstore.log.appender.compaction.combiner;

import java.util.PriorityQueue;

public class NonUniqueMergeCombiner<T extends Comparable<T>> extends MergeCombiner<T> {

    private final PriorityQueue<IteratorContainer<T>> queue = new PriorityQueue<>();

    @Override
    protected IteratorContainer<T> pollFirst() {
        return queue.poll();
    }

    @Override
    protected void add(IteratorContainer<T> item) {
        queue.add(item);
    }

    @Override
    protected boolean isEmpty() {
        return queue.isEmpty();
    }
}
