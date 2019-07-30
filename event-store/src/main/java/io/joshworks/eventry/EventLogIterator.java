package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.io.IOException;
import java.util.function.Function;

public class EventLogIterator implements Streamable<EventRecord> {

    private final LogIterator<EventRecord> delegate;
    private EventRecord last;

    public EventLogIterator(LogIterator<EventRecord> delegate, Function<EventRecord, EventRecord> resolver, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        LogIterator<EventRecord> policyFiltered = Iterators.filtering(delegate, ev -> {
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
        Function<EventRecord, EventRecord> mapping = LinkToPolicy.RESOLVE.equals(linkToPolicy) ? resolver : ev -> ev;
        this.delegate = Iterators.mapping(policyFiltered, mapping);
    }

    public StreamName lastEvent() {
        return last == null ? null : StreamName.from(last);
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
        EventRecord event = delegate.next();
        if (event != null) {
            last = event;
        }
        return event;
    }
}
