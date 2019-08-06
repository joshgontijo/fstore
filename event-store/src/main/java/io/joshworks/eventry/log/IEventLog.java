package io.joshworks.eventry.log;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;

import java.io.Closeable;

public interface IEventLog extends Closeable {
    long append(EventRecord event);

    EventRecord get(long position);

    long entries();

    void close();

    EventStoreIterator iterator(Direction direction);

    EventStoreIterator iterator(Direction direction, EventMap checkpoint);

    void compact();
}
