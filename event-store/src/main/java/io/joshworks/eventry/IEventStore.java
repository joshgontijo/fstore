package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.JsonEvent;
import io.joshworks.eventry.projections.State;
import io.joshworks.fstore.log.LogIterator;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

public interface IEventStore extends Closeable, IProjection, IStream, IStreamQuery, ILogIterator, IEventAppender {

    void cleanup();

    void compactIndex();

    State query(Set<String> streams, State state, String script);

    @Override
    void close();
}
