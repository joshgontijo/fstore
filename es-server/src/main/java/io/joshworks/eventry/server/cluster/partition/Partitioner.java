package io.joshworks.eventry.server.cluster.partition;

public interface Partitioner {

    int select(String stream, int numPartitions);

}
