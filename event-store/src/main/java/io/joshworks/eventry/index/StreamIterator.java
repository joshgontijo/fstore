package io.joshworks.eventry.index;

import io.joshworks.eventry.StreamListener;
import io.joshworks.fstore.log.CloseableIterator;

public interface StreamIterator extends StreamListener, CloseableIterator<IndexEntry>
{
    boolean hasNext();

    IndexEntry next();

    void close();
}
