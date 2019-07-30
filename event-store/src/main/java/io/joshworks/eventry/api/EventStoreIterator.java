package io.joshworks.eventry.api;

import io.joshworks.eventry.Streamable;
import io.joshworks.eventry.index.Checkpoint;
import io.joshworks.eventry.log.EventRecord;

public interface EventStoreIterator extends Streamable<EventRecord> {

    Checkpoint checkpoint();
}
