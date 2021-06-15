package io.joshworks.es2.directory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CompactionItem<T extends SegmentFile> {
    final View<T> view;
    final File replacement;
    final List<T> sources;
    final int sourceLevel;
    final int nextLevel;

    boolean success;
    long created = System.currentTimeMillis();
    long startTs;
    long endTs;


    CompactionItem(View<T> view, File replacement, List<T> sources, int sourceLevel, int nextLevel) {
        this.view = view;
        this.replacement = replacement;
        this.sources = sources;
        this.sourceLevel = sourceLevel;
        this.nextLevel = nextLevel;
    }

    public int sourceLevel() {
        return sourceLevel;
    }

    public int nextLevel() {
        return nextLevel;
    }

    public File replacement() {
        return replacement;
    }

    public List<T> sources() {
        return new ArrayList<>(sources);
    }

    @Override
    public String toString() {
        return sources.stream().map(SegmentFile::name).collect(Collectors.toList()) + " -> " + replacement.getName();
    }
}
