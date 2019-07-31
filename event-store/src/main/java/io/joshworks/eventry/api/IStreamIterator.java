package io.joshworks.eventry.api;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.EventMap;

import java.util.Set;

public interface IStreamIterator {

    EventStoreIterator fromStream(EventId stream);

    EventStoreIterator fromStreams(EventMap checkpoint, Set<String> streamPatterns);

    EventStoreIterator fromStreams(EventMap streams);
}
