package io.joshworks.eventry.log;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.LogPoller;

import java.io.Closeable;
import java.util.stream.Stream;

public interface IEventLog extends Closeable {
    long append(EventRecord event);

    EventRecord get(long position);

    long entries();

    long position();

    void close();

    LogIterator<EventRecord> iterator(Direction direction);

    Stream<EventRecord> stream(Direction direction);

    LogPoller<EventRecord> poller();

    LogPoller<EventRecord> poller(long position);

    void cleanup();
}
