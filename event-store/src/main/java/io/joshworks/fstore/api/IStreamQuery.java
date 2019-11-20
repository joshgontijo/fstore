package io.joshworks.fstore.api;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;

public interface IStreamQuery {

    EventRecord get(EventId stream);

    int version(String stream);

    int count(String stream);

}
