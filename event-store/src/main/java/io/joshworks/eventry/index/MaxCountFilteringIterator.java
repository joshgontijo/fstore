package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.CloseableIterator;

import java.util.function.Function;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;

class MaxCountFilteringIterator implements CloseableIterator<IndexEntry> {

    private final Function<Long, Integer> versionFetcher;
    private final IndexIterator delegate;
    private final Function<Long, StreamMetadata> metadataSupplier;
    private IndexEntry next;

    MaxCountFilteringIterator(Function<Long, StreamMetadata> metadataSupplier, Function<Long, Integer> versionFetcher, IndexIterator delegate) {
        this.metadataSupplier = metadataSupplier;
        this.versionFetcher = versionFetcher;
        this.delegate = delegate;
    }


    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        next = dropEvents();
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

    private IndexEntry dropEvents() {
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

}