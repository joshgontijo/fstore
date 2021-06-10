package io.joshworks.es2.directory;

public interface Compaction<T extends SegmentFile> {

    void compact(CompactionItem<T> handle);
}
