package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexIterator;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.IEventLog;

/**
 * Random access log iterator
 */
public class IndexedLogIterator implements IndexIterator {

    private final IndexIterator indexIterator;
    private final IEventLog log;

    IndexedLogIterator(IndexIterator indexIterator, IEventLog log) {
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
    public void close() {
        indexIterator.close();
    }
}
