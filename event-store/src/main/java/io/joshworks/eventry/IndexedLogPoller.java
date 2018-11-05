package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.PollingSubscriber;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

class IndexedLogPoller implements PollingSubscriber<EventRecord> {

    private final EventStore store;
    private final PollingSubscriber<IndexEntry> indexPoller;

    IndexedLogPoller(PollingSubscriber<IndexEntry> indexPoller, EventStore store) {
        this.indexPoller = indexPoller;
        this.store = store;
    }

    private EventRecord getOrElse(IndexEntry peek) {
        return Optional.ofNullable(peek).map(store::get).orElse(null);
    }

    @Override
    public EventRecord peek() throws InterruptedException {
        IndexEntry peek = indexPoller.peek();
        return getOrElse(peek);
    }

    @Override
    public EventRecord poll() throws InterruptedException {
        IndexEntry poll = indexPoller.poll();
        return getOrElse(poll);
    }

    @Override
    public EventRecord poll(long limit, TimeUnit timeUnit) throws InterruptedException {
        IndexEntry poll = indexPoller.poll(limit, timeUnit);
        return getOrElse(poll);
    }

    @Override
    public EventRecord take() throws InterruptedException {
        IndexEntry take = indexPoller.take();
        return getOrElse(take);
    }

    @Override
    public boolean headOfLog() {
        return indexPoller.headOfLog();
    }

    @Override
    public boolean endOfLog() {
        return indexPoller.endOfLog();
    }

    @Override
    public long position() {
        return -1; //TODO this is not reliable for index log
    }

    @Override
    public void close() throws IOException {
        indexPoller.close();
    }
}
