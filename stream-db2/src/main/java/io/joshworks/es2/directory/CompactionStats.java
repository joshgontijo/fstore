package io.joshworks.es2.directory;

public class CompactionStats {

    private final long createdTime;
    private final long queueTime;
    private final long executionTime;

    private final long sourceSize;
    private final long outSize;


    public <T extends SegmentFile> CompactionStats(CompactionItem<T> item) {
        this.createdTime = item.created;
        this.queueTime = item.startTs - item.created;
        this.executionTime = item.endTs - item.startTs;
        this.sourceSize = item.sources().stream().mapToLong(SegmentFile::size).sum();
        this.outSize = item.replacement.length();
    }
}
