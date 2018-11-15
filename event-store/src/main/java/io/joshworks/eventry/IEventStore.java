package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;

import java.io.Closeable;

public interface IEventStore extends Closeable, IProjection, IStream, IStreamQuery, IEventPoller, IEventAppender {

    void cleanup();

    void compactIndex();

}
