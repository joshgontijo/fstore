package io.joshworks.eventry;

import io.joshworks.eventry.index.Checkpoint;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.function.Function;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;

public class MaxAgeFilteringIterator implements StreamIterator {

    private final StreamIterator delegate;
    private final Function<Long, Integer> metadataSupplier;
    private EventRecord next;

    MaxAgeFilteringIterator(Function<Long, Integer> metadataSupplier, StreamIterator delegate) {
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
    public StreamData next() {
        StreamData record = takeWhile();
        next = null;
        return record;
    }

    private EventRecord takeWhile() {
        if (next != null) {
            return next;
        }
        StreamData last = nextEntry();
        while (last != null && !withinMaxAge(last)) {
            last = nextEntry();
        }
        return last != null && withinMaxAge(last) ? last : null;
    }

    private boolean withinMaxAge(StreamData event) {
        StreamMetadata metadata = metadataSupplier.apply(event.entry.stream);
        long maxAge = metadata.maxAgeSec;
        long now = System.currentTimeMillis();
        long diff = ((now - event.timestamp) / 1000);
        return maxAge <= NO_MAX_AGE || diff <= maxAge;
    }

    private StreamData nextEntry() {
        return delegate.hasNext() ? delegate.next() : null;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public Checkpoint checkpoint() {
        return delegate.checkpoint();
    }
}