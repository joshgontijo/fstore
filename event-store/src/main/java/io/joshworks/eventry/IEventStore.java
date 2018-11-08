package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;

import java.io.Closeable;

public interface IEventStore extends Closeable, IProjection, IStream, IStreamQuery, IEventPoller {

    void cleanup();

    void compactIndex();

    EventRecord linkTo(String stream, EventRecord event);

    void emit(String stream, EventRecord event);

    EventRecord append(EventRecord event);

    EventRecord append(EventRecord event, int expectedVersion);

    @Override
    void close();
}
