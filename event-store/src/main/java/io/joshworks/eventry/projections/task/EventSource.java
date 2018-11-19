package io.joshworks.eventry.projections.task;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

class EventSource implements Closeable {

    private final LogIterator<EventRecord> iterator;
    private final Set<String> streams;
    private final StreamTracker tracker = new StreamTracker();


    EventSource(LogIterator<EventRecord> iterator, Set<String> streams) {
        this.iterator = iterator;
        this.streams = streams;
    }

    public Set<String> streams() {
        return streams;
    }

    public EventRecord next() {
        return tracker.update(iterator.next());
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public long position() {
        return iterator.position();
    }

    public Map<String, Integer> streamTracker() {
        return tracker.tracker();
    }

    public void close() throws IOException {
        iterator.close();
    }
}
