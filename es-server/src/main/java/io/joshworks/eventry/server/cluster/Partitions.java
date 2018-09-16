package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventStore;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

public class Partitions {

    private final NavigableMap<Long, EventStore> stores = new TreeMap<>();
    private final Function<String, Long> hasher;

    public Partitions(Function<String, Long> hasher) {
        this.hasher = hasher;
    }

    public void addPartition(long partitionStart, EventStore store) {
        stores.put(partitionStart, store);
    }

    public EventStore remove(long partitionStart) {
        return stores.remove(partitionStart);
    }

    public EventStore select(String stream) {
        return select(hasher.apply(stream));
    }

    public EventStore select(long streamHash) {
        Map.Entry<Long, EventStore> entry = stores.floorEntry(streamHash);
        if(entry == null) {
            throw new IllegalStateException("No node found for hash " + streamHash);
        }
        return entry.getValue();
    }

    public Stream<EventStore> stream() {
        return stores.values().stream();
    }

    public void onNodeDiscovered() {

    }

}
