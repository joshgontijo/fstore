package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.eventry.log.EventRecord;

public interface NodeEvent {
    EventRecord toEvent();
}
