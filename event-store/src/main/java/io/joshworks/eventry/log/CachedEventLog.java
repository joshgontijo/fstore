package io.joshworks.eventry.log;

import io.joshworks.eventry.log.cache.EventRecordCache;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;

public class CachedEventLog implements IEventLog {

    private final IEventLog delegate;
    private final EventRecordCache cache;

    public CachedEventLog(IEventLog delegate, long maxSize, int maxAgeSec) {
        this.delegate = delegate;
        this.cache = EventRecordCache.instance(maxSize, maxAgeSec);
    }

    public long append(EventRecord event) {
        return delegate.append(event);
    }

    public EventRecord get(long position) {
        EventRecord cached = cache.get(position);
        if (cached != null) {
            return cached;
        }
        EventRecord event = delegate.get(position);
        if (event == null) {
            throw new IllegalArgumentException("No event found for " + position);
        }
        cache.cache(position, event);
        return event;
    }

    @Override
    public long entries() {
        return delegate.entries();
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public LogIterator<EventRecord> iterator(Direction direction) {
        return delegate.iterator(direction);
    }

    @Override
    public LogIterator<EventRecord> iterator(Direction direction, long position) {
        return delegate.iterator(direction, position);
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
    }

}
