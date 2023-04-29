package io.joshworks.es2.directory;

public interface Compaction<T extends SegmentFile> {

    static <T extends SegmentFile> Compaction<T> noOp() {
        return handle -> {
        };
    }

    void compact(CompactionItem<T> handle);

}
