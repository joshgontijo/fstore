package io.joshworks.fstore.log;

import java.util.stream.Stream;

public interface LogIterator<T> extends CloseableIterator<T>, IPosition {

    default Stream<T> stream() {
        return Iterators.closeableStream(this);
    }
}
