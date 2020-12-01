package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.joshworks.es2.directory.DirectoryUtils.level;
import static io.joshworks.es2.directory.DirectoryUtils.segmentFileName;
import static io.joshworks.es2.directory.DirectoryUtils.segmentIdx;

/**
 * Not Thread safe, synchronization must be done outside
 * Mainly because EventStore needs to sync on both Log and Index a the same time, adding here would just be unecessary overhead
 * head() does not need sync as it does not change
 */
public class SegmentDirectory<T extends SegmentFile> {

    private volatile View<T> view = new View<>();

    private final File root;
    private final String extension;

    public SegmentDirectory(File root, String extension) {
        this.root = root;
        this.extension = extension;
        initDirectory(root);
    }

    private synchronized void replaceView(View<T> newView) {
        View<T> current = this.view;
        this.view = newView;
        current.close();
    }

    public synchronized File newHead() {
        try (View<T> view = this.view.acquire()) {
            long segIdx = 0;
            if (!view.isEmpty()) {
                T currentHead = view.head();
                int headLevel = level(currentHead);
                if (headLevel == 0) {
                    segIdx = segmentIdx(currentHead) + 1;
                }
            }
            return newSegmentFile(segIdx, 0);
        }
    }

    //modifications must be synchronized
    public synchronized void append(T newSegmentHead) {
        try (View<T> view = this.view.acquire()) {
            if (view.head().compareTo(newSegmentHead) <= 0) {
                throw new IllegalStateException("New segment won't be new head");
            }

            View<T> newView = view.append(newSegmentHead);
            assert newView.head().equals(newSegmentHead);

            replaceView(newView);
        }
    }

    private static void initDirectory(File root) {
        try {
            if (!root.isDirectory()) {
                throw new IllegalArgumentException("Not a directory " + root.getAbsolutePath());
            }
            Files.createDirectories(root.toPath());
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize segment directory", e);
        }
    }

    public void loadSegments(Function<File, T> fn) {
        try {
            List<T> items = Files.list(root.toPath())
                    .map(Path::toFile)
                    .filter(this::matchExtension)
                    .map(fn)
                    .collect(Collectors.toList());

            replaceView(new View<>(items));

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to load segments", e);
        }

    }

    //TODO close View create new one
    private synchronized void merge(MergeHandle<T> merge) {
        try (View<T> view = this.view.acquire()) {

            Set<T> files = new HashSet<>(merge.sources);
            T replacement = merge.replacement;

            int expectedNextLevel = computeNextLevel(files);
            long expectedSegmentIdx = files.stream().mapToLong(DirectoryUtils::segmentIdx).min().getAsLong();

            int replacementLevel = level(replacement);
            long replacementIdx = segmentIdx(replacement);
            if (replacementLevel != expectedNextLevel) {
                throw new IllegalArgumentException("Expected level " + expectedNextLevel + ", got " + replacementLevel);
            }
            if (replacementIdx != expectedSegmentIdx) {
                throw new IllegalArgumentException("Expected segmentIdx " + expectedSegmentIdx + ", got " + replacementIdx);
            }

            View<T> newView = view.merge(merge);
            replaceView(newView);
        }
    }

    public int computeNextLevel(Set<T> files) {
        return files.stream().mapToInt(DirectoryUtils::level).max().getAsInt() + 1;
    }

    public File root() {
        return root;
    }

    public synchronized MergeHandle<T> createMerge(int level, int maxItems) {
        try (View<T> view = this.view.acquire()) {
            view.createMerge()

        }
    }

    private boolean matchExtension(File file) {
        return file.getName().endsWith(extension);
    }

    public void close() {
        try (View<T> view = this.view.acquire()) {
            replaceView(new View<>());
        }
    }

    private File newSegmentFile(long segmentIdx, int level) {
        String name = segmentFileName(segmentIdx, level, extension);
        return new File(root, name);
    }


}
