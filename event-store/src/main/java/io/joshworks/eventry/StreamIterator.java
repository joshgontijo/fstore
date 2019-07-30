package io.joshworks.eventry;

import io.joshworks.eventry.index.Checkpoint;
import io.joshworks.eventry.log.EventRecord;

public interface StreamIterator extends Streamable<EventRecord> {

    Checkpoint checkpoint();
}
