package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogPoller;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Event log poller that returns all the events, including linkTo.
 * In case of link events, it resolves to the actual event
 */
class EventLogPoller implements LogPoller<EventRecord> {

    private final EventStore store;
    private final LogPoller<EventRecord> delegate;
    private final LinkToPolicy linkToPolicy;
    private final SystemEventPolicy systemEventPolicy;

    private EventRecord last;
    private boolean hasPeeked;

    EventLogPoller(LogPoller<EventRecord> delegate, EventStore store, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        this.store = store;
        this.delegate = delegate;
        this.linkToPolicy = linkToPolicy;
        this.systemEventPolicy = systemEventPolicy;
    }

    private EventRecord getNext() throws InterruptedException {
        if (hasPeeked) {
            return last;
        }
        do {
            EventRecord record = delegate.poll();
            if (eventMatches(record)) {
                return resolve(record);
            }
        } while (!delegate.headOfLog());
        return null;
    }

    private boolean eventMatches(EventRecord record) {
        if (record == null) {
            return false;
        }
        if (LinkToPolicy.IGNORE.equals(linkToPolicy) && record.isLinkToEvent()) {
            return false;
        }
        if (SystemEventPolicy.IGNORE.equals(systemEventPolicy) && record.isSystemEvent()) {
            return false;
        }
        return true;
    }

    private EventRecord resolve(EventRecord record) {
        if (record != null && LinkToPolicy.RESOLVE.equals(linkToPolicy)) {
            return store.resolve(record);
        }
        return record;
    }

    @Override
    public synchronized EventRecord peek() throws InterruptedException {
        EventRecord record = getNext();
        hasPeeked = record != null;
        last = record;
        return record;
    }

    @Override
    public synchronized EventRecord poll() throws InterruptedException {
        EventRecord next = getNext();
        hasPeeked = false;
        return next;
    }

    @Override
    public synchronized EventRecord poll(long limit, TimeUnit timeUnit) throws InterruptedException {
        EventRecord next = getNext();
        hasPeeked = false;
        if (next == null) {
            EventRecord record = delegate.poll(limit, timeUnit);
            return resolve(record);
        }
        return next;
    }

    @Override
    public synchronized EventRecord take() throws InterruptedException {
        EventRecord next = getNext();
        hasPeeked = false;
        if (next == null) {
            EventRecord record = delegate.take();
            return resolve(record);
        }
        return next;
    }

    @Override
    public synchronized boolean headOfLog() {
        if (hasPeeked && last != null) {
            return delegate.headOfLog();
        }
        try {
            EventRecord peeked = peek();
            return peeked == null && delegate.headOfLog();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean endOfLog() {
        return false;
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
