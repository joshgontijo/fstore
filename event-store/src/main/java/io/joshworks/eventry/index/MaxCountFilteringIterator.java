package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;

import java.util.function.Function;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;

class MaxCountFilteringIterator implements StreamIterator {

    private final Function<Long, Integer> versionFetcher;
    private final StreamIterator delegate;
    private final Function<Long, StreamMetadata> metadataSupplier;
    private IndexEntry next;

    MaxCountFilteringIterator(Function<Long, StreamMetadata> metadataSupplier,
                              Function<Long, Integer> versionFetcher,
                              StreamIterator delegate) {
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
        } while (last != null && !withinMaxCount(last));
        return last != null && withinMaxCount(last) ? last : null;
    }

    private IndexEntry nextEntry() {
        return delegate.hasNext() ? delegate.next() : null;
    }

    //count is based on stream version rather than event count
    private boolean withinMaxCount(IndexEntry last) {
        StreamMetadata metadata = metadataSupplier.apply(last.stream);
        return metadata.maxCount <= NO_MAX_COUNT || last.version > (versionFetcher.apply(last.stream) - metadata.maxCount);
    }

    @Override
    public void close() {
        delegate.close();
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