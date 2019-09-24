package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.lsmtree.sstable.Entry;

class IndexRangeIterator implements IndexIterator {

    private final CloseableIterator<Entry<IndexKey, Long>> delegate;
    private final long stream;
    private int version;

    IndexRangeIterator(CloseableIterator<Entry<IndexKey, Long>> delegate, long stream, int version) {
        this.delegate = delegate;
        this.stream = stream;
        this.version = version;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public IndexEntry next() {
        Entry<IndexKey, Long> next = delegate.next();
        if (next == null) {
            return null;
        }
        IndexEntry ie = IndexEntry.of(next.key.stream, next.key.version, next.value, next.timestamp);
        version = ie.version;
        return ie;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public EventMap checkpoint() {
        return EventMap.of(stream, version);
    }

    @Override
    public void onStreamCreated(StreamMetadata metadata) {

    }

    @Override
    public void onStreamTruncated(StreamMetadata metadata) {

    }

    @Override
    public void onStreamDeleted(StreamMetadata metadata) {

    }
}
