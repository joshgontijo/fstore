package io.joshworks.es2.directory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CompactionItem<T extends SegmentFile> {
    final View<T> view;
    final File replacement;
    final List<T> sources;
    long created = System.currentTimeMillis();
    long startTs;
    long endTs;


    CompactionItem(View<T> view, File replacement, List<T> sources) {
        this.view = view;
        this.replacement = replacement;
        this.sources = sources;
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
