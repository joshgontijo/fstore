package io.joshworks.eventry.partition;

public interface Partitioner {

    int select(String stream, int numPartitions);

}
