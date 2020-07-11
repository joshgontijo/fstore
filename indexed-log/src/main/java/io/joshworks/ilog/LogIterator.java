package io.joshworks.ilog;

import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.ilog.record.Record;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class LogIterator implements Iterators.CloseableIterator<Record> {

    private final Queue<SegmentIterator> iterators = new ArrayDeque<>();

    public LogIterator(List<SegmentIterator> iterators) {
        this.iterators.addAll(iterators);
    }

    public static LogIterator empty() {
        return new LogIterator(new ArrayList<>());
    }

    @Override
    public boolean hasNext() {
        SegmentIterator curr = iterators.peek();
        if (curr == null) {
            return false;
        }
        if (!curr.hasNext()) {
            iterators.poll();
            return hasNext();
        }
        return true;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        SegmentIterator peeked = iterators.peek();
        return peeked == null ? null : peeked.next();
    }

    @Override
    public void close() {
        for (SegmentIterator iterator : iterators) {
            iterator.close();
        }
    }

    public void add(SegmentIterator iterator) {
        iterators.add(iterator);
    }
}
