package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;

/**
 * Random access log iterator
 */
public class IndexedLogIterator implements EventLogIterator {

    private final LogIterator<IndexEntry> indexIterator;
    private final IEventLog log;

    public IndexedLogIterator(LogIterator<IndexEntry> indexIterator, IEventLog log) {
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
