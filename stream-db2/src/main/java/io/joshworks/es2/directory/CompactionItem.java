package io.joshworks.es2.directory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class CompactionItem<T extends SegmentFile> {
    final View<T> view;
    final File replacement;
    final List<T> sources;
    final int sourceLevel;

    boolean success;
    long created = System.currentTimeMillis();
    long startTs;
    long endTs;


    CompactionItem(View<T> view, File replacement, List<T> sources, int sourceLevel) {
        this.view = view;
        this.replacement = replacement;
        this.sources = sources;
        this.sourceLevel = sourceLevel;
    }

    public int nextLevel() {
        return sourceLevel + 1;
    }

    public File replacement() {
        return replacement;
    }

    public List<T> sources() {
        return new ArrayList<>(sources);
    }

    @Override
    public String toString() {
        return sources.stream().map(SegmentFile::name).toList() + " -> " + replacement.getName();
    }
}
