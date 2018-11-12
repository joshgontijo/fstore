package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogPoller;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

class EventLogPoller implements LogPoller<EventRecord> {

    private final EventStore store;
    private final LogPoller<EventRecord> logPoller;

    EventLogPoller(LogPoller<EventRecord> logPoller, EventStore store) {
        this.store = store;
        this.logPoller = logPoller;
    }

    @Override
    public EventRecord peek() throws InterruptedException {
        return store.resolve(logPoller.peek());
    }

    @Override
    public EventRecord poll() throws InterruptedException {
        return store.resolve(logPoller.poll());
    }

    @Override
    public EventRecord poll(long limit, TimeUnit timeUnit) throws InterruptedException {
        return store.resolve(logPoller.poll(limit, timeUnit));
    }

    @Override
    public EventRecord take() throws InterruptedException {
        return store.resolve(logPoller.take());
    }

    @Override
    public boolean headOfLog() {
        return logPoller.headOfLog();
    }

    @Override
    public boolean endOfLog() {
        return false;
    }

    @Override
    public long position() {
        return logPoller.position();
    }

    @Override
    public void close() throws IOException {
        logPoller.close();
    }
}
