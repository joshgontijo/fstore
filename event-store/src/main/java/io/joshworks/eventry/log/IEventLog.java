package io.joshworks.eventry.log;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;

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

    PollingSubscriber<EventRecord> poller();

    PollingSubscriber<EventRecord> poller(long position);

    void cleanup();
}
