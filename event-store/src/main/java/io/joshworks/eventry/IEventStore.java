package io.joshworks.eventry;

import java.io.Closeable;

public interface IEventStore extends Closeable, IStream, IStreamQuery, ILogIterator, IEventAppender {

    void compact();

    @Override
    void close();
}
