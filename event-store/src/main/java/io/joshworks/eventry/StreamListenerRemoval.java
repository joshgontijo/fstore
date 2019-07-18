package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.StreamIterator;
import io.joshworks.eventry.stream.StreamMetadata;

import java.util.Set;

public class StreamListenerRemoval implements StreamIterator {

    private final Set<StreamListener> listeners;
    private final StreamIterator delegate;

    public StreamListenerRemoval(Set<StreamListener> listeners, StreamIterator delegate) {
        this.listeners = listeners;
        this.delegate = delegate;
        this.listeners.add(delegate);
    }

    @Override
    public void close() {
        listeners.remove(delegate);
        delegate.close();
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public IndexEntry next() {
        return delegate.next();
    }

    @Override
    public void onStreamCreated(StreamMetadata metadata) {
        delegate.onStreamCreated(metadata);
    }

    @Override
    public void onStreamTruncated(StreamMetadata metadata) {
        delegate.onStreamTruncated(metadata);
    }

    @Override
    public void onStreamDeleted(StreamMetadata metadata) {
        delegate.onStreamDeleted(metadata);
    }
}
