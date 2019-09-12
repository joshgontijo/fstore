package io.joshworks.eventry.log;

import io.joshworks.fstore.core.metrics.MetricRegistry;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;

import java.util.Map;

public class EventLog implements IEventLog {

    private final LogAppender<EventRecord> appender;
    private final String metricsKey;

    public EventLog(LogAppender<EventRecord> appender) {
        this.appender = appender;
        this.metricsKey = MetricRegistry.register(Map.of("name", appender.name(), "type", "event-log"), appender::metrics);
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
        MetricRegistry.remove(metricsKey);
        appender.close();
    }

    @Override
    public LogIterator<EventRecord> iterator(Direction direction) {
        return appender.iterator(direction);
    }

    @Override
    public LogIterator<EventRecord> iterator(Direction direction, long position) {
        return appender.iterator(direction, position);
    }

    @Override
    public void compact() {
        appender.compact();
    }

}
