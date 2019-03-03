package io.joshworks.eventry.projection.task;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;

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

    Set<StreamName> get() {
        return multiStreamTracker.entrySet().stream().map(e -> StreamName.of(e.getKey(), e.getValue())).collect(Collectors.toSet());
    }

}
