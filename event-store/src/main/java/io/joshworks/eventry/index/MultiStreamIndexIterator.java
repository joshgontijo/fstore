package io.joshworks.eventry.index;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.util.List;
import java.util.NoSuchElementException;

public class MultiStreamIndexIterator implements IndexIterator {

    private final List<? extends IndexIterator> iterators;
    private final boolean ordered;
    private LogIterator<IndexEntry> delegate;

    public MultiStreamIndexIterator(List<? extends IndexIterator> iterators, boolean ordered) {
        this.iterators = iterators;
        this.ordered = ordered;
        this.delegate = newIterator();
    }

    private LogIterator<IndexEntry> newIterator() {
        return ordered ? Iterators.ordered(iterators) : Iterators.concat(iterators);
    }

    @Override
    public boolean hasNext() {
        if(!delegate.hasNext()) {
            this.delegate = newIterator();
        }
        return delegate.hasNext();
    }

    @Override
    public IndexEntry next() {
        if (!delegate.hasNext()) {
            delegate = newIterator();
            if (!delegate.hasNext()) {
                return null;
            }
        }
        return delegate.next();
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public void close() {
        iterators.forEach(IOUtils::closeQuietly);
    }

    @Override
    public Checkpoint processed() {
        return iterators.stream().map(IndexIterator::processed)
                .reduce(Checkpoint.empty(), Checkpoint::merge);
    }
}
