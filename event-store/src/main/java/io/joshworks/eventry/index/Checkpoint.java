package io.joshworks.eventry.index;

import io.joshworks.eventry.StreamName;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.joshworks.eventry.log.EventRecord.NO_VERSION;

public class Checkpoint {

    private final Map<Long, Integer> map = new HashMap<>();

    public void put(long stream, int version) {
        map.put(stream, version);
    }

    public Checkpoint merge(Checkpoint other) {
        Checkpoint copy = new Checkpoint();
        copy.map.putAll(map);

        other.map.forEach((key, value) -> {
            long stream = key;
            int version = value;
            copy.map.merge(stream, version, (v1, v2) -> v1 > v2 ? v1 : v2);
        });

        return copy;
    }

    public static Checkpoint of(long stream) {
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.put(stream, NO_VERSION);
        return checkpoint;
    }

    public static Checkpoint of(Set<Long> streams) {
        Checkpoint checkpoint = new Checkpoint();
        for (Long stream : streams) {
            checkpoint.put(stream, NO_VERSION);
        }
        return checkpoint;
    }

    public static Checkpoint of(long stream, int lastReadVersion) {
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.put(stream, lastReadVersion);
        return checkpoint;
    }

    public static Checkpoint from(Set<StreamName> streamNames) {
        Checkpoint checkpoint = new Checkpoint();
        for (StreamName streamName : streamNames) {
            checkpoint.put(streamName.hash(), streamName.version());
        }
        return checkpoint;
    }

    public static Checkpoint from(StreamName streamName) {
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.put(streamName.hash(), streamName.version());
        return checkpoint;
    }


    public static Checkpoint empty() {
        return new Checkpoint();
    }

    //TODO improve this part

    public int size() {
        return map.size();
    }

    public Iterable<? extends Map.Entry<Long, Integer>> entrySet() {
        return map.entrySet();
    }


    public Set<Long> keySet() {
        return map.keySet();
    }
}
