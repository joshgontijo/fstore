package io.joshworks.fstore.api;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventMap;

import java.util.Set;

public interface IStreamIterator {

    EventStoreIterator fromStream(EventId stream);

    EventStoreIterator fromStreams(EventMap checkpoint, Set<String> streamPatterns);

    EventStoreIterator fromStreams(EventMap streams);
}
