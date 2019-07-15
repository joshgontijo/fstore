package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.IndexIterator;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.IEventLog;

import java.io.IOException;

/**
 * Random access log iterator
 */
public class IndexedLogIterator implements EventLogIterator {

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
    public long position() {
        //TODO should this be supported ?
        //for streams it doesnt make sense, for _all it does
        //split iterators in two ? stream iterator, log iterator ?
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void close() throws IOException {
        indexIterator.close();
    }
}
