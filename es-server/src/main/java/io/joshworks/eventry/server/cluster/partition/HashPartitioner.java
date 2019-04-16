package io.joshworks.eventry.server.cluster.partition;

import io.joshworks.eventry.StreamName;

public class HashPartitioner implements Partitioner {

    @Override
    public int select(String stream, int numPartitions) {
        long hash = StreamName.hash(stream);
        return (int) (Math.abs(hash) % numPartitions);
    }
}
