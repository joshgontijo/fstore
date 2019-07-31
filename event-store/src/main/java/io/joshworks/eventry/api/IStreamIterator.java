package io.joshworks.eventry.api;

import io.joshworks.eventry.StreamName;

import java.util.Set;

public interface IStreamIterator {

    EventStoreIterator fromStream(StreamName stream);

    EventStoreIterator fromStreams(String... streamPatterns);

    EventStoreIterator fromStreams(Set<StreamName> streams);
}
