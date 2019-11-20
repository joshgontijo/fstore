package io.joshworks.fstore.api;

import io.joshworks.fstore.Streamable;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;

public interface EventStoreIterator extends Streamable<EventRecord> {

    EventMap checkpoint();
}
