package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.function.Function;

class MappingIterator<R, T> implements LogIterator<R> {

    private final LogIterator<T> delegate;
    private Function<T, R> mapper;

    MappingIterator(LogIterator<T> delegate, Function<T, R> mapper) {
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
