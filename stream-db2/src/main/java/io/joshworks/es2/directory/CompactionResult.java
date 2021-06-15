package io.joshworks.es2.directory;

import java.util.ArrayList;
import java.util.Arrays;

public class CompactionResult extends ArrayList<CompactionStats> {

    public CompactionResult() {
    }

    public CompactionResult(CompactionStats item) {
        super(Arrays.asList(item));
    }

    public static CompactionResult merge(CompactionResult r1, CompactionResult r2) {
        r1.addAll(r2);
        return r1;
    }
}
