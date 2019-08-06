package io.joshworks.fstore.es.shared;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;


public class EventMap {

    private final Map<Long, Integer> map = new ConcurrentHashMap<>();

    public static EventMap empty() {
        return new EventMap();
    }

    public static EventMap of(long stream) {
        return empty().add(stream, NO_VERSION);
    }

    public static EventMap of(Set<Long> streams) {
        EventMap eventMap = new EventMap();
        for (Long stream : streams) {
            eventMap.add(stream, NO_VERSION);
        }
        return eventMap;
    }

    public static EventMap of(long streamHash, int version) {
        return empty().add(streamHash, version);
    }

    public static EventMap from(Set<EventId> eventIds) {
        EventMap eventMap = new EventMap();
        for (EventId eventId : eventIds) {
            eventMap.add(eventId);
        }
        return eventMap;
    }

    public static EventMap from(EventId eventId) {
        return empty().add(eventId.hash(), eventId.version());
    }


    public EventMap add(long stream, int version) {
        map.put(stream, version);
        return this;
    }

    public EventMap add(EventId eventId) {
        map.put(eventId.hash(), eventId.version());
        return this;
    }

    public EventMap merge(EventMap other) {
        EventMap copy = new EventMap();
        copy.map.putAll(map);

        other.map.forEach((key, value) -> {
            long stream = key;
            int version = value;
            copy.map.merge(stream, version, (v1, v2) -> v1 > v2 ? v1 : v2);
        });

        return copy;
    }

    //TODO improve this part

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Set<Map.Entry<Long, Integer>> entrySet() {
        return map.entrySet();
    }

    public Set<Long> streamHashes() {
        return map.keySet();
    }

    public int get(long stream) {
        return map.getOrDefault(stream, NO_VERSION);
    }

    public Iterator<Map.Entry<Long, Integer>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
