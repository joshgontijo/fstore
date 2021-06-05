package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MergeHandle<T extends SegmentFile> {
    final View<T> view;
    final File replacement;
    final List<T> sources;

    MergeHandle(View<T> view, File replacement, List<T> sources) {
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

}
