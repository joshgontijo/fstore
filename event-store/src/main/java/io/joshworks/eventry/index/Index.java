package io.joshworks.eventry.index;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;

import java.io.Closeable;
import java.util.Optional;
import java.util.stream.Stream;

public interface Index extends Closeable {

    LogIterator<IndexEntry> indexIterator(Direction direction);

    LogIterator<IndexEntry> indexIterator(Direction direction, Range range);

    Stream<IndexEntry> indexStream(Direction direction);

    Stream<IndexEntry> indexStream(Direction direction, Range range);

    Optional<IndexEntry> get(long stream, int version);

//    void delete(long stream);

    int version(long stream);
}