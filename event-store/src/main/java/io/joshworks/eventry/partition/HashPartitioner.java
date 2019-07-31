package io.joshworks.eventry.partition;

import io.joshworks.eventry.EventId;

public class HashPartitioner implements Partitioner {

    @Override
    public int select(String stream, int numPartitions) {
        long hash = EventId.hash(stream);
        return (int) (Math.abs(hash) % numPartitions);
    }
}
