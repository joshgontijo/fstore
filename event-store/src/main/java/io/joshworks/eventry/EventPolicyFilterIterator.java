package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;

public class EventPolicyFilterIterator implements EventLogIterator {

    private final LogIterator<EventRecord> delegate;

    public EventPolicyFilterIterator(EventLogIterator delegate, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        this.delegate = Iterators.filtering(delegate, ev -> {
            if (ev == null) {
                return false;
            }
            if (LinkToPolicy.IGNORE.equals(linkToPolicy) && ev.isLinkToEvent()) {
                return false;
            }
            if (SystemEventPolicy.IGNORE.equals(systemEventPolicy) && ev.isSystemEvent() && !ev.isLinkToEvent()) {
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
