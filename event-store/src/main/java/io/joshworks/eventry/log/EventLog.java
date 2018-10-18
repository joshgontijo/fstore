package io.joshworks.eventry.log;

import io.joshworks.eventry.log.cache.EventRecordCache;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.util.stream.Stream;

public class EventLog implements Closeable {

    private final LogAppender<EventRecord> appender;
    private final EventRecordCache cache;

    public EventLog(Config<EventRecord> config) {
        this.appender = config.open();
        this.cache = EventRecordCache.instance(Size.MB.of(500), 120);
    }

    public long append(EventRecord event) {
        return appender.append(event);
    }

    public EventRecord get(long position) {
        EventRecord cached = cache.get(position);
        if(cached != null) {
            return cached;
        }
        EventRecord event = appender.get(position);
        if (event == null) {
            throw new IllegalArgumentException("No event found for " + position);
        }
        cache.cache(position, event);
        return event;
    }

    public long entries() {
        return appender.entries();
    }

    public long position() {
        return appender.position();
    }

    @Override
    public void close() {
        appender.close();
    }

    public LogIterator<EventRecord> iterator(Direction direction) {
        return appender.iterator(direction);
    }

    public Stream<EventRecord> stream(Direction direction) {
        return Iterators.closeableStream(iterator(direction));
    }

    public PollingSubscriber<EventRecord> poller() {
        return appender.poller();
    }

    public PollingSubscriber<EventRecord> poller(long position) {
        return appender.poller(position);
    }

    public void cleanup() {
        appender.compact();
    }

}
