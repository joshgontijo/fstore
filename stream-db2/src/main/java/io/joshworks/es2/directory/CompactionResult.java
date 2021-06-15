package io.joshworks.es2.directory;

import java.util.ArrayList;

public class CompactionResult extends ArrayList<CompactionStats> {

    public CompactionResult() {
    }

    public <T extends SegmentFile> CompactionResult(CompactionItem<T> item) {
        add(new CompactionStats(
                item.created,
                item.startTs - item.created,
                item.endTs - item.startTs,
                item.sources().stream().mapToLong(SegmentFile::size).sum(),
                item.replacement.length()
        ));
    }

    public static CompactionResult merge(CompactionResult r1, CompactionResult r2) {
        r1.addAll(r2);
        return r1;
    }
}
