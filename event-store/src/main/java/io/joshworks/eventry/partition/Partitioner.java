package io.joshworks.eventry.partition;

public interface Partitioner {

    int select(long streamHash, int numPartitions);

}
