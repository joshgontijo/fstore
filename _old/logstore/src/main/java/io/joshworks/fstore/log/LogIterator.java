package io.joshworks.fstore.log;

import io.joshworks.fstore.log.iterators.Iterators;

import java.util.stream.Stream;

public interface LogIterator<T> extends CloseableIterator<T>, IPosition {

    default Stream<T> stream() {
        return Iterators.closeableStream(this);
    }
}
