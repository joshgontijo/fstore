package io.joshworks.eventry;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.util.function.Function;

public class EventLogIterator implements EventStoreIterator {

    private final LogIterator<EventRecord> delegate;

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

    @Override
    public void close() {
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

    @Override
    public EventMap checkpoint() {
        //Using Event map with position instead of
        return EventMap.of(delegate.position());
    }
}
