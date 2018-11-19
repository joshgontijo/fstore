package io.joshworks.eventry.log;

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
    public long position() {
        return appender.position();
    }

    @Override
    public void close() {
        appender.close();
    }

    @Override
    public LogIterator<EventRecord> iterator(Direction direction) {
        return appender.iterator(direction);
    }


    @Override
    public void cleanup() {
        appender.compact();
    }

}
