package io.joshworks.eventry;

import io.joshworks.eventry.index.Checkpoint;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.IndexIterator;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.IEventLog;

import java.util.function.Function;

/**
 * Random access log iterator
 */
public class IndexedLogIterator implements StreamIterator {

    private final IndexIterator indexIterator;
    private final IEventLog log;
    private final Function<EventRecord, EventRecord> resolver;

    IndexedLogIterator(IndexIterator indexIterator, IEventLog log, Function<EventRecord, EventRecord> resolver) {
        this.indexIterator = indexIterator;
        this.log = log;
        this.resolver = resolver;
    }

    @Override
    public boolean hasNext() {
        return indexIterator.hasNext();
    }

    @Override
    public EventRecord next() {
        IndexEntry indexEntry = indexIterator.next();
        if (indexEntry == null) {
            return null;
        }
        EventRecord record = log.get(indexEntry.position);
        return resolver.apply(record);
    }

    @Override
    public void close() {
        indexIterator.close();
    }

    @Override
    public Checkpoint checkpoint() {
        return indexIterator.checkpoint();
    }
}
