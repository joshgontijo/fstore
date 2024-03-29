package io.joshworks.fstore;

import io.joshworks.fstore.api.EventStoreIterator;
import io.joshworks.fstore.index.IndexEntry;
import io.joshworks.fstore.index.IndexIterator;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.log.IEventLog;
import io.joshworks.fstore.es.shared.EventMap;

import java.util.function.Function;

/**
 * Random access log iterator
 */
public class IndexedLogIterator implements EventStoreIterator {

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
    public EventMap checkpoint() {
        return indexIterator.checkpoint();
    }
}
