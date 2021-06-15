package io.joshworks.es2.directory;

public class CompactionStats {

    public final long createdTime;
    public final long queueTime;
    public final long executionTime;

    public final long sourceSize;
    public final long outSize;

    public CompactionStats(long createdTime, long queueTime, long executionTime, long sourceSize, long outSize) {
        this.createdTime = createdTime;
        this.queueTime = queueTime;
        this.executionTime = executionTime;
        this.sourceSize = sourceSize;
        this.outSize = outSize;
    }
}
