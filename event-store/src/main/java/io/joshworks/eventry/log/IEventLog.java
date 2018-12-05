package io.joshworks.eventry.log;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;

import java.io.Closeable;

public interface IEventLog extends Closeable {
    long append(EventRecord event);

    EventRecord get(long position);

    long entries();

    long position();

    void close();

    LogIterator<EventRecord> iterator(Direction direction);

    LogIterator<EventRecord> iterator(Direction direction, long position);

    void compact();
}
