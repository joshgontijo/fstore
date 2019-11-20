package io.joshworks.fstore.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;

public interface NodeEvent {
    EventRecord toEvent();
}
