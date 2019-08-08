package io.joshworks.eventry.api;

import io.joshworks.eventry.Streamable;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;

public interface EventStoreIterator extends Streamable<EventRecord> {

    EventMap checkpoint();
}
