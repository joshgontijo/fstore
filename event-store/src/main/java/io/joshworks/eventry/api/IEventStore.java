package io.joshworks.eventry.api;

import java.io.Closeable;

public interface IEventStore extends Closeable, IStream, IStreamQuery, IStreamIterator, ILogIterator, IEventAppender {

    void compact();

    @Override
    void close();
}
