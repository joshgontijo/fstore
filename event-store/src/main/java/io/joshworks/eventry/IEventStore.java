package io.joshworks.eventry;

import io.joshworks.eventry.projections.State;

import java.io.Closeable;
import java.util.Set;

public interface IEventStore extends Closeable, IProjection, IStream, IStreamQuery, ILogIterator, IEventAppender {

    void compact();

    State query(Set<String> streams, State state, String script);

    @Override
    void close();
}
