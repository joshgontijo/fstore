package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.CloseableIterator;

import java.util.function.Function;

/**
 * filters out entries if stream was truncated after acquiring the iterator
 */
@Deprecated
class TruncatedAwareIterator implements CloseableIterator<IndexEntry> {

    private final StreamIterator delegate;
    private final Function<Long, StreamMetadata> metadataSupplier;
    private IndexEntry next;

    TruncatedAwareIterator(Function<Long, StreamMetadata> metadataSupplier, StreamIterator delegate) {
        this.metadataSupplier = metadataSupplier;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        next = takeWhile();
        return next != null;
    }

    @Override
    public IndexEntry next() {
        IndexEntry entry = takeWhile();
        next = null;
        return entry;
    }

    private IndexEntry takeWhile() {
        if (next != null) {
            return next;
        }
        IndexEntry last = nextEntry();
        while (last != null && !afterTruncation(last)) {
            last = nextEntry();
        }
        return last != null && afterTruncation(last) ? last : null;
    }

    private boolean afterTruncation(IndexEntry event) {
        StreamMetadata metadata = metadataSupplier.apply(event.stream);
        if (!metadata.truncated()) {
            return true;
        }
        return !metadata.truncated() || event.version > metadata.truncated;
    }

    private IndexEntry nextEntry() {
        return delegate.hasNext() ? delegate.next() : null;
    }

    @Override
    public void close() {
        delegate.close();
    }

}
