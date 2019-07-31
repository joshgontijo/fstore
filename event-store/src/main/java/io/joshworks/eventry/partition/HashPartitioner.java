package io.joshworks.eventry.partition;

import io.joshworks.eventry.EventId;

public class HashPartitioner implements Partitioner {

    @Override
    public int select(long streamHash, int numPartitions) {
        return (int) (Math.abs(streamHash) % numPartitions);
    }
}
