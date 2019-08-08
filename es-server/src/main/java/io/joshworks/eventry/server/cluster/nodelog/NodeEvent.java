package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;

public interface NodeEvent {
    EventRecord toEvent();
}
