package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class MergeHandle<T extends SegmentFile> {
    final T replacement;
    final File replacementFile;
    final List<T> sources;

    MergeHandle(File replacementFile, T replacement, List<T> sources) {
        this.replacementFile = replacementFile;
        this.replacement = replacement;
        this.sources = sources;
    }

    public T replacement() {
        return replacement;
    }

    public List<T> sources() {
        return new ArrayList<>(sources);
    }

    public File replacementFile() {
        return replacementFile;
    }
}
