package io.joshworks.es2.directory;

public class CompactionStats {

    private final long createdTime;
    private final long queueTime;
    private final long executionTime;

    //in bytes
    private final long sourceSize;
    private final long outSize;

    private final long sourceEntries;
    private final long outEntries;

    public <T extends SegmentFile> CompactionStats(CompactionItem<T> item) {
        this.createdTime = item.created;
        this.queueTime = item.startTs - item.created;
        this.executionTime = item.endTs - item.startTs;
        this.sourceSize = item.sources().stream().map(SegmentFile::)

    }

    public static CompactionStats merge(CompactionStats c1, CompactionStats c2) {
        return null;
    }
}
