package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;

public class SingleStreamIterator implements LogIterator<EventRecord> {

    private final LogIterator<IndexEntry> indexIterator;
    private final IEventLog log;

    public SingleStreamIterator(LogIterator<IndexEntry> indexIterator, IEventLog log) {
        this.indexIterator = indexIterator;
        this.log = log;
    }

    @Override
    public boolean hasNext() {
        return indexIterator.hasNext();
    }

    @Override
    public EventRecord next() {
        IndexEntry indexEntry = indexIterator.next();
        return log.get(indexEntry.position);
    }

    @Override
    public long position() {
        return indexIterator.position();
    }

    @Override
    public void close() throws IOException {
        indexIterator.close();
    }
}
