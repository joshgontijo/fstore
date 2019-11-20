package io.joshworks.fstore.log;

import io.joshworks.fstore.es.shared.EventRecord;

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
