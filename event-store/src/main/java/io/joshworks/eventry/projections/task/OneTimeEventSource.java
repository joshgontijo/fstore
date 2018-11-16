package io.joshworks.eventry.projections.task;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

class OneTimeEventSource implements EventSource {

    private final LogIterator<EventRecord> iterator;
    private final Set<String> streams;
    private final StreamTracker tracker = new StreamTracker();


    OneTimeEventSource(LogIterator<EventRecord> iterator, Set<String> streams) {
        this.iterator = iterator;
        this.streams = streams;
    }

    @Override
    public Set<String> streams() {
        return streams;
    }

    @Override
    public EventRecord next() {
        return tracker.update(iterator.next());
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public long position() {
        return iterator.position();
    }

    @Override
    public Map<String, Integer> streamTracker() {
        return tracker.tracker();
    }

    @Override
    public void close() throws IOException {
        iterator.close();
    }
}
