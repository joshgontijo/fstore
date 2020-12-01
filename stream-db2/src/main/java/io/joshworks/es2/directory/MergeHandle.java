package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;

import java.io.Closeable;
import java.io.File;
import java.util.List;

public class MergeHandle<T extends SegmentFile> implements Closeable {
    final File replacement;
    final List<T> sources;
    final View<T> ref;


    MergeHandle(File replacement, List<T> sources, View<T> ref) {
        this.replacement = replacement;
        this.sources = sources;
        this.ref = ref;
    }

    @Override
    public void close() {
        ref.
        ref.close();
    }
}
