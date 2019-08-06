package io.joshworks.fstore.client;

import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.JsonEvent;

import java.io.Closeable;
import java.util.Iterator;

public interface ClientStreamIterator extends Iterator<JsonEvent>, Closeable {

    EventMap checkpoint();

    @Override
    void close();

}
