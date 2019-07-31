package io.joshworks.eventry.api;

import io.joshworks.eventry.Streamable;
import io.joshworks.eventry.EventMap;
import io.joshworks.eventry.log.EventRecord;

public interface EventStoreIterator extends Streamable<EventRecord> {

    EventMap checkpoint();
}
