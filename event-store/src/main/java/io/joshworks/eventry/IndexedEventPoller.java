package io.joshworks.eventry;

import io.joshworks.eventry.index.TableIndex;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogPoller;

import java.util.Map;
import java.util.concurrent.TimeUnit;

class IndexedEventPoller implements LogPoller<EventRecord> {

    private final EventStore store;
    private final TableIndex.IndexPoller indexPoller;

    IndexedEventPoller(TableIndex.IndexPoller indexPoller, EventStore store) {
        this.store = store;
        this.indexPoller = indexPoller;
    }

    public Map<Long, Integer> processed() {
        return indexPoller.processed();
    }

    @Override
    public EventRecord peek() {
        return store.get(indexPoller.peek());
    }

    @Override
    public EventRecord poll() {
        return store.get(indexPoller.poll());
    }

    @Override
    public EventRecord poll(long limit, TimeUnit timeUnit) throws InterruptedException {
        return store.get(indexPoller.poll(limit, timeUnit));
    }

    @Override
    public EventRecord take() throws InterruptedException {
        return store.get(indexPoller.take());
    }

    @Override
    public boolean headOfLog() {
        return indexPoller.headOfLog();
    }

    @Override
    public boolean endOfLog() {
        return false;
    }

    @Override
    public long position() {
        return indexPoller.position();
    }

    @Override
    public void close() {
        indexPoller.close();
    }
}
