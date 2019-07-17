package io.joshworks.eventry;

import io.joshworks.eventry.index.StreamIterator;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.IEventLog;

import java.io.IOException;

/**
 * Random access log iterator
 */
public class IndexedLogIterator implements EventLogIterator {

    private final StreamIterator streamIterator;
    private final IEventLog log;

    IndexedLogIterator(StreamIterator streamIterator, IEventLog log) {
        this.streamIterator = streamIterator;
        this.log = log;
    }

    @Override
    public boolean hasNext() {
        return streamIterator.hasNext();
    }

    @Override
    public EventRecord next() {
        IndexEntry indexEntry = streamIterator.next();
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
        streamIterator.close();
    }
}
