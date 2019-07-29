package io.joshworks.eventry;

import io.joshworks.eventry.index.Checkpoint;
import io.joshworks.fstore.log.CloseableIterator;

public interface StreamIterator extends CloseableIterator<StreamData> {

    Checkpoint checkpoint();
}
