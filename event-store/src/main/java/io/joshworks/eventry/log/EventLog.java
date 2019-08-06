package io.joshworks.eventry.log;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.LogAppender;

public class EventLog implements IEventLog {

    private final LogAppender<EventRecord> appender;

    public EventLog(Config<EventRecord> config) {
        this.appender = config.open();
    }

    @Override
    public long append(EventRecord event) {
        return appender.append(event);
    }

    @Override
    public EventRecord get(long position) {
        EventRecord event = appender.get(position);
        if (event == null) {
            throw new IllegalArgumentException("No event found for " + position);
        }
        return event;
    }

    @Override
    public long entries() {
        return appender.entries();
    }

    @Override
    public void close() {
        appender.close();
    }

    @Override
    public EventStoreIterator iterator(Direction direction) {
        return new IteratorWrapper(appender.iterator(direction));
    }

    @Override
    public EventStoreIterator iterator(Direction direction, EventMap checkpoint) {
        long pos = checkpoint.iterator().next().getKey(); //position instead stream
        return new IteratorWrapper(appender.iterator(direction, pos));
    }

    @Override
    public void compact() {
        appender.compact();
    }

    private static class IteratorWrapper implements EventStoreIterator {

        private final LogIterator<EventRecord> delegate;

        private IteratorWrapper(LogIterator<EventRecord> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public EventRecord next() {
            return delegate.next();
        }

        @Override
        public EventMap checkpoint() {
            return EventMap.of(delegate.position());
        }
    }

}
