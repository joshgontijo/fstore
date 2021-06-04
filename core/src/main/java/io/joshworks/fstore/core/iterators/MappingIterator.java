package io.joshworks.fstore.core.iterators;

import java.util.function.Function;

class MappingIterator<R, T> implements CloseableIterator<R> {

    private final CloseableIterator<T> delegate;
    private Function<T, R> mapper;

    MappingIterator(CloseableIterator<T> delegate, Function<T, R> mapper) {
        this.delegate = delegate;
        this.mapper = mapper;
    }

    @Override
    public void close() {
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
