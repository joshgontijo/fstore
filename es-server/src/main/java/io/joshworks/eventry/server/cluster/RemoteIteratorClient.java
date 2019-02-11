package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;

public class RemoteIteratorClient implements LogIterator<EventRecord> {

    private final RecordChannel channel;

    @Override
    public long position() {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public EventRecord next() {
        return null;
    }
}
