package io.joshworks.fstore;

import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Streamable<T> extends CloseableIterator<T> {

    default Stream<T> stream() {
        return Iterators.closeableStream(this);
    }

    default List<T> toList() {
        return stream().collect(Collectors.toList());
    }
}
