package io.joshworks.es.log;

import io.joshworks.fstore.core.util.Iterators;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class LogIterator implements Iterators.CloseableIterator<ByteBuffer> {

    private final Queue<SegmentIterator> iterators = new ArrayDeque<>();
    private final Consumer<LogIterator> onClose;

    private final AtomicLong tmpAddress = new AtomicLong();

    public LogIterator(List<SegmentIterator> iterators, Consumer<LogIterator> onClose) {
        this.iterators.addAll(iterators);
        this.onClose = onClose;
    }

    @Override
    public boolean hasNext() {
        SegmentIterator curr = iterators.peek();
        if (curr == null) {
            return false;
        }
        if (!curr.hasNext() && curr.endOfLog()) {
            SegmentIterator completed = iterators.poll();
            if (completed != null) {
                tmpAddress.set(completed.address());
            }
            return hasNext();
        }
        return true;
    }

    @Override
    public ByteBuffer next() {
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
        onClose.accept(this);
    }

    public long address() {
        if (iterators.isEmpty()) {
            return tmpAddress.get();
        }
        return iterators.peek().address();
    }

    void add(SegmentIterator iterator) {
        iterators.add(iterator);
    }
}
