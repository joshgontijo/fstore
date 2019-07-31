package io.joshworks.eventry.index;

import io.joshworks.eventry.EventMap;
import io.joshworks.eventry.EventUtils;
import io.joshworks.eventry.stream.StreamMetadata;

import java.util.function.Function;

/**
 * Filters out maxAge and maxCount entries
 */
class IndexFilter implements IndexIterator {

    private final Function<Long, Integer> versionFetcher;
    private final IndexIterator delegate;
    private final Function<Long, StreamMetadata> metadataSupplier;
    private IndexEntry next;

    IndexFilter(Function<Long, StreamMetadata> metadataSupplier,
                Function<Long, Integer> versionFetcher,
                IndexIterator delegate) {
        this.metadataSupplier = metadataSupplier;
        this.versionFetcher = versionFetcher;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        next = dropWhile();
        return next != null;
    }

    @Override
    public IndexEntry next() {
        if (!hasNext()) {
            return null;
        }
        IndexEntry temp = next;
        next = null;
        return temp;
    }

    private IndexEntry dropWhile() {
        IndexEntry last;
        do {
            last = nextEntry();
        } while (last != null && !validEntry(last));
        return last != null && validEntry(last) ? last : null;
    }

    private IndexEntry nextEntry() {
        return delegate.hasNext() ? delegate.next() : null;
    }

    //count is based on stream version rather than event count
    private boolean validEntry(IndexEntry last) {
        StreamMetadata metadata = metadataSupplier.apply(last.stream);
        return EventUtils.validIndexEntry(metadata, last.version, last.timestamp, versionFetcher);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public EventMap checkpoint() {
        return delegate.checkpoint();
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