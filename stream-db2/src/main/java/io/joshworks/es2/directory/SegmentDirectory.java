package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.joshworks.es2.directory.DirectoryUtils.deleteAllWithExtension;
import static io.joshworks.es2.directory.DirectoryUtils.initDirectory;
import static io.joshworks.es2.directory.DirectoryUtils.level;
import static io.joshworks.es2.directory.DirectoryUtils.segmentFileName;
import static io.joshworks.es2.directory.DirectoryUtils.segmentIdx;
import static java.lang.Math.min;

/**
 * Not Thread safe, synchronization must be done outside
 * Mainly because EventStore needs to sync on both Log and Index a the same time, adding here would just be unecessary overhead
 * head() does not need sync as it does not change
 */
public class SegmentDirectory<T extends SegmentFile> implements Iterable<T>, Closeable {

    private static final String TMP = "tmp";

    private final AtomicReference<View<T>> viewRef = new AtomicReference<>(new View<>());
    private final Set<T> merging = new HashSet<>();

    private final File root;
    private final Function<File, T> supplier;
    private final String extension;
    private final ExecutorService executor;
    private final Compaction<T> compaction;

    public SegmentDirectory(File root,
                            Function<File, T> supplier,
                            String extension,
                            ExecutorService executor,
                            Compaction<T> compaction) {

        this.root = root;
        this.supplier = supplier;
        this.extension = extension;
        this.executor = executor;
        this.compaction = compaction;

        initDirectory(root);
        deleteAllWithExtension(root, TMP);
    }


    public File newHead() {
        long segIdx = 0;
        var view = this.viewRef.get();
        if (!view.isEmpty()) {
            T currentHead = view.head();
            int headLevel = level(currentHead);
            if (headLevel == 0) {
                segIdx = segmentIdx(currentHead) + 1;
            }
        }
        return createFile(0, segIdx, extension);
    }

    private File createFile(int level, long segIdx, String ext) {
        String name = segmentFileName(segIdx, level, ext);
        return new File(root, name);
    }

    public synchronized void append(T newSegmentHead) {
        var currView = this.viewRef.get();
        if (!currView.isEmpty() && currView.head().compareTo(newSegmentHead) <= 0) {
            throw new IllegalStateException("Invalid segment head");
        }
        View<T> newView = currView.add(newSegmentHead);
        swapView(newView);
    }

    private void move(File src, File dst) {
        try {
            Files.move(src.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to rename file", e);
        }
    }

    private void swapView(View<T> view) {
        viewRef.getAndSet(view).close();
    }

    public void loadSegments() {
        try {
            List<T> segments = Files.list(root.toPath())
                    .map(Path::toFile)
                    .filter(this::matchExtension)
                    .map(supplier)
                    .collect(Collectors.toList());
            swapView(new View<>(segments));
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to load segments", e);
        }
    }

    //TODO move parameters to constructor
    public synchronized CompletableFuture<Void> compact(int minItems, int maxItems) {
        var view = this.viewRef.get();

        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        for (var level = 0; level <= maxLevel(view); level++) {

            T head = view.head();
            List<T> levelSegments = levelSegments(view, level)
                    .stream()
                    .filter(s -> !merging.contains(s))
                    .filter(s -> !head.equals(s))
                    .collect(Collectors.toList());

            int nextLevel = level + 1;
            do {
                int items = min(maxItems, levelSegments.size());
                var sublist = new ArrayList<>(levelSegments.subList(0, items));
                levelSegments.removeAll(sublist);

                //TODO can have duplicate file names
                var replacementFile = createFile(nextLevel, maxIdx(view, nextLevel), TMP);
                var handle = new MergeHandle<>(view.acquire(), replacementFile, sublist);

                merging.addAll(sublist);
                tasks.add(submit(handle));

            } while (levelSegments.size() >= minItems);
        }

        return CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> submit(MergeHandle<T> handle) {
        return CompletableFuture.runAsync(() -> safeCompact(handle), executor)
                .thenRun(() -> completeMerge(handle));
    }

    //TODO handle error cases, view must always be closed afterwards
    private void safeCompact(MergeHandle<T> handle) {
        try {
            compaction.compact(handle);
        } catch (Exception e) {
            try {
                Files.delete(handle.replacement().toPath());
            } catch (IOException err) {
                System.err.println("Failed to delete failed compaction result file: " + handle.replacement().getAbsolutePath());
                err.printStackTrace();
            }
            //TODO logging
            e.printStackTrace();
        }
    }

    private synchronized void completeMerge(MergeHandle<T> handle) {
        var currentView = viewRef.get();
        try {
            var file = handle.replacement();
            assert file.getName().endsWith(TMP);

            String newFileName = file.getName().replaceAll("\\." + TMP, "." + extension);
            var path = file.toPath();
            var mergeOut = Files.move(path, path.getParent().resolve(newFileName)).toFile();
            List<T> sources = handle.sources();

            long mergeOutLen = mergeOut.length();
            var replacement = mergeOutLen > 0 ? supplier.apply(mergeOut) : null;

            handle.view.close(); //release merge handle view
            View<T> mergedView = currentView.replace(sources, replacement);
            if (mergeOutLen == 0) {
                Files.delete(mergeOut.toPath());
            }

            swapView(mergedView);
            handle.sources.forEach(merging::remove);

        } catch (Exception e) {
            //TODO add logging
            //TODO fail execution and mark handle ?
            System.err.println("FAILED TO MERGE");
            e.printStackTrace();
        }
    }

    private static <T extends SegmentFile> List<T> levelSegments(View<T> view, int level) {
        try (view) {
            return view.stream()
                    .filter(l -> level(l) == level)
                    .collect(Collectors.toList());
        }
    }

    private static <T extends SegmentFile> long maxLevel(View<T> view) {
        try (view) {
            return view.stream()
                    .mapToInt(DirectoryUtils::level)
                    .max()
                    .orElse(0);
        }
    }

    private static <T extends SegmentFile> long maxIdx(View<T> view, int level) {
        //do not change, other methods are iterating the view stream, it needs to be closed
        List<T> levelSegments = levelSegments(view, level);
        return levelSegments
                .stream()
                .mapToLong(DirectoryUtils::segmentIdx)
                .max()
                .orElse(0);
    }

    private boolean matchExtension(File file) {
        return file.getName().endsWith(extension);
    }

    @Override
    public synchronized void close() {
        this.viewRef.getAndSet(new View<>()).close();
    }

    @Override
    public Iterator<T> iterator() {
        return this.viewRef.get().iterator();
    }

    public Iterator<T> reverse() {
        return this.viewRef.get().reverse();
    }

    public void delete() {
        try (var view = viewRef.getAndSet(new View<>())) {
            view.deleteAll();
        }
    }

}
