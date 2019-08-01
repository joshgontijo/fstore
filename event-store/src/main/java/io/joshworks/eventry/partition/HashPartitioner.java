package io.joshworks.eventry.partition;

public class HashPartitioner implements Partitioner {

    @Override
    public int select(long streamHash, int buckets) {
        return (int) (Math.abs(streamHash) % buckets);
    }
}
