package io.joshworks.fstore.client;

import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;

import java.io.Closeable;
import java.util.Iterator;

public interface ClientStreamIterator extends Iterator<EventRecord>, Closeable {

    EventMap checkpoint();

    @Override
    void close();

}
