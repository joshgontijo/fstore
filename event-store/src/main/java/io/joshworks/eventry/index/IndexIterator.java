package io.joshworks.eventry.index;

import io.joshworks.eventry.StreamListener;
import io.joshworks.fstore.log.CloseableIterator;

public interface IndexIterator extends StreamListener, CloseableIterator<IndexEntry> {

    boolean hasNext();

    IndexEntry next();

    void close();

    Checkpoint checkpoint();
}
