package io.joshworks.eventry.index;

import io.joshworks.eventry.StreamName;

import java.util.HashMap;
import java.util.Set;

import static io.joshworks.eventry.log.EventRecord.NO_VERSION;

public class Checkpoint extends HashMap<Long, Integer> {

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

    public static Checkpoint from(Set<StreamName> streamNames) {
        Checkpoint checkpoint = new Checkpoint();
        for (StreamName streamName : streamNames) {
            checkpoint.put(streamName.hash(), streamName.version());
        }
        return checkpoint;
    }

    public static Checkpoint of(long stream, int lastReadVersion) {
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.put(stream, lastReadVersion);
        return checkpoint;
    }

    public static Checkpoint empty() {
        return new Checkpoint();
    }

    public Checkpoint merge(Checkpoint other) {
        Checkpoint copy = new Checkpoint();
        copy.putAll(this);
        this.putAll(other);
        return copy;
    }

}
