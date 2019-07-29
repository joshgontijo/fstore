package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.io.IOException;

public class EventPolicyFilterIterator implements LogIterator<EventRecord> {

    private final LogIterator<EventRecord> delegate;

    public EventPolicyFilterIterator(LogIterator<EventRecord> delegate, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        this.delegate = Iterators.filtering(delegate, ev -> {
            if (ev == null) {
                return false;
            }
            if (LinkToPolicy.IGNORE.equals(linkToPolicy) && ev.isLinkToEvent()) {
                return false;
            }
            if (SystemEventPolicy.IGNORE.equals(systemEventPolicy) && ev.isSystemEvent()) {
                return false;
            }
            return true;
        });
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public EventRecord next() {
        return delegate.next();
    }
}
