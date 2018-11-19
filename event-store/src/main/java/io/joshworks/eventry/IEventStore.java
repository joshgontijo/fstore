package io.joshworks.eventry;

import java.io.Closeable;

public interface IEventStore extends Closeable, IProjection, IStream, IStreamQuery, ILogIterator, IEventAppender {

    void cleanup();

    void compactIndex();

    @Override
    void close();
}
