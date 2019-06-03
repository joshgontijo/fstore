package io.joshworks.eventry;

import io.joshworks.eventry.index.Checkpoint;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.io.IOUtils;

public class CheckpointEventLogIterator implements EventLogIterator {

    private final EventLogIterator delegate;
    private final Checkpoint checkpoint = Checkpoint.empty();

    public CheckpointEventLogIterator(EventLogIterator delegate) {
        this.delegate = delegate;
    }

    public Checkpoint checkpoint() {
        return checkpoint;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public EventRecord next() {
        EventRecord next = delegate.next();
        if (next != null) {
            long hash = StreamName.hash(next.stream);
            checkpoint.put(hash, next.version);
        }
        return next;
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(delegate);
    }

}
