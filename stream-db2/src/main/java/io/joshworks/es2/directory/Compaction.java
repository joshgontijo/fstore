package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;

public interface Compaction<T extends SegmentFile> {

    void compact(MergeHandle<T> handle);
}
