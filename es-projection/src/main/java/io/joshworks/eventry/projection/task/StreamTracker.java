package io.joshworks.fstore.projection.task;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class StreamTracker {

    private final Map<String, Integer> multiStreamTracker = new ConcurrentHashMap<>();

    EventRecord update(EventRecord record) {
        if (record != null) {
            multiStreamTracker.put(record.stream, record.version);
        }
        return record;
    }

    Set<EventId> get() {
        return multiStreamTracker.entrySet().stream().map(e -> EventId.of(e.getKey(), e.getValue())).collect(Collectors.toSet());
    }

}
