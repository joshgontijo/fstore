package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.function.Function;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;

public class MaxAgeFilteringIterator implements EventLogIterator {

    private final LogIterator<EventRecord> delegate;
    private final Function<String, StreamMetadata> metadataSupplier;
    private EventRecord next;

    MaxAgeFilteringIterator(Function<String, StreamMetadata> metadataSupplier, LogIterator<EventRecord> delegate) {
        this.metadataSupplier = metadataSupplier;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        next = takeWhile();
        return next != null;
    }

    //guaranteed to return an element if hasNext evaluated to true
    @Override
    public EventRecord next() {
        EventRecord record = takeWhile();
        next = null;
        return record;
    }

    private EventRecord takeWhile() {
        if (next != null) {
            return next;
        }
        EventRecord last = nextEntry();
        while (last != null && !withinMaxAge(last)) {
            last = nextEntry();
        }
        return last != null && withinMaxAge(last) ? last : null;
    }

    private boolean withinMaxAge(EventRecord event) {
        StreamMetadata metadata = metadataSupplier.apply(event.stream);
        long maxAge = metadata.maxAge;
        long now = System.currentTimeMillis();
        long diff = ((now - event.timestamp) / 1000);
        return maxAge <= NO_MAX_AGE || diff <= maxAge;
    }

    private EventRecord nextEntry() {
        return delegate.hasNext() ? delegate.next() : null;
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}