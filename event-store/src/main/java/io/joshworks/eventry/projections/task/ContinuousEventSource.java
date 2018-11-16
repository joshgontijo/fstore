package io.joshworks.eventry.projections.task;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogPoller;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class ContinuousEventSource implements EventSource {

    private final LogPoller<EventRecord> poller;
    private final Set<String> streams;
    private final StreamTracker tracker = new StreamTracker();

    ContinuousEventSource(LogPoller<EventRecord> poller, Set<String> streams) {
        this.poller = poller;
        this.streams = streams;
    }

    @Override
    public Set<String> streams() {
        return streams;
    }

    @Override
    public EventRecord next() {
        try {
            EventRecord record = poller.poll(5, TimeUnit.SECONDS);
            return tracker.update(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return !poller.headOfLog();
    }

    @Override
    public Map<String, Integer> streamTracker() {
        return tracker.tracker();
    }

    @Override
    public long position() {
        return poller.position();
    }

    @Override
    public void close() throws IOException {
        poller.close();
    }
}
