package io.joshworks.es2.directory;

public interface Compaction<T extends SegmentFile> {

    void compact(MergeHandle<T> handle);
}
