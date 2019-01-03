package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.Map;

public class MaxAgeFilteringIterator implements EventLogIterator {

    private final LogIterator<EventRecord> delegate;
    private EventRecord next;
    private final long timestamp = System.currentTimeMillis();
    private Map<String, Long> maxAges;

    MaxAgeFilteringIterator(Map<String, Long> metadataMap, LogIterator<EventRecord> delegate) {
        this.maxAges = metadataMap;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        next = takeWhile();
        return next != null;
    }

    @Override
    public EventRecord next() {
        EventRecord record = takeWhile();
        next = null;
        return record;
    }

    private EventRecord takeWhile() {
        if(next != null) {
            return next;
        }
        EventRecord last = nextEntry();
        while(last != null && !withinMaxAge(last)) {
            last = nextEntry();
        }
        return last != null && withinMaxAge(last) ? last : null;
    }

    private boolean withinMaxAge(EventRecord event) {
        Long maxAge = maxAges.get(event.stream);
        return maxAge == null || maxAge <= 0 || ((timestamp - event.timestamp) / 1000) <= maxAge;
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