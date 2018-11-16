package io.joshworks.eventry.projections.task;

import io.joshworks.eventry.log.EventRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class StreamTracker {

    private final Map<String, Integer> multiStreamTracker = new ConcurrentHashMap<>();

    EventRecord update(EventRecord record) {
        if (record != null) {
            multiStreamTracker.put(record.stream, record.version);
        }
        return record;
    }

    Map<String, Integer> tracker() {
        return new HashMap<>(multiStreamTracker);
    }

}
