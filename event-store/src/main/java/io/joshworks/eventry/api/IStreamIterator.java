package io.joshworks.eventry.api;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.EventMap;

public interface IStreamIterator {

    EventStoreIterator fromStream(EventId stream);

    EventStoreIterator fromStreams(EventMap checkpoint, String... streamPatterns);

    EventStoreIterator fromStreams(EventMap streams);
}
