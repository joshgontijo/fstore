package io.joshworks.eventry.server.cluster.node;

public interface Partitioner {

    int select(long streamHash, int buckets);

}
