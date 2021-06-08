package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.joshworks.es2.directory.DirectoryUtils.initDirectory;
import static io.joshworks.es2.directory.DirectoryUtils.segmentFileName;
import static io.joshworks.es2.directory.DirectoryUtils.segmentId;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class SegmentDirectory<T extends SegmentFile> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SegmentDirectory.class);

    public static final String METADATA_EXT = "metadata";

    private final AtomicReference<View<T>> viewRef = new AtomicReference<>(new View<>());
    private final Set<MergeHandle<T>> compacting = new HashSet<>();

    private final File root;
    private final Metadata<T> metadata;
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
        this.metadata = new Metadata<>(new File(root, extension + "." + METADATA_EXT));

        initDirectory(root);
//        deleteAllWithExtension(root);
    }


    public File newHead() {
        long segIdx = 0;
        var view = this.viewRef.get();
        if (!view.isEmpty()) {
            var id = segmentId(view.head());
            segIdx = id.level() == 0 ? id.idx() + 1 : segIdx;
        }
        return createFile(0, segIdx);
    }

    private File createFile(int level, long segIdx) {
        String name = segmentFileName(segIdx, level, extension);
        return new File(root, name);
    }

    public synchronized void append(T newSegmentHead) {
        var currView = this.viewRef.get();
        if (!currView.isEmpty() && currView.head().compareTo(newSegmentHead) <= 0) {
            throw new IllegalStateException("Invalid segment head");
        }
        metadata.add(
                newSegmentHead
        );
        View<T> newView = currView.add(newSegmentHead);
        swapView(newView);
    }

    private void swapView(View<T> view) {
        viewRef.getAndSet(view).close();
    }

    public void loadSegments() {
        try {
            List<T> segments = metadata.state()
                    .stream()
                    .map(segId -> segmentFileName(segId.idx(), segId.level(), extension))
                    .map(name -> new File(root, name))
                    .map(supplier)
                    .collect(Collectors.toList());

            for (T segment : segments) {
                log.info("Loaded {}", segment);
            }

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
                    .filter(seg -> eligibleForCompaction(view, seg))
                    .filter(s -> !head.equals(s))
                    .collect(Collectors.toList());
            Collections.reverse(levelSegments); //reverse so we start from oldest files first

            int nextLevel = level + 1;
            do {
                int items = min(maxItems, levelSegments.size());
                var sublist = new ArrayList<>(levelSegments.subList(0, items));
                levelSegments.removeAll(sublist);

                var replacementFile = createFile(nextLevel, nextIdx(view, nextLevel));
                var handle = new MergeHandle<>(view.acquire(), replacementFile, sublist);

                System.err.println("Submitting " + handle);
                compacting.add(handle);
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
            var outFile = handle.replacement();

            T out = supplier.apply(outFile);
            List<T> sources = handle.sources();

            long mergeOutLen = outFile.length();
            if (mergeOutLen > 0) {//merge output has data replace
                View<T> mergedView = currentView.replace(sources, out);
                metadata.merge(
                        out,
                        handle.sources
                );
                swapView(mergedView);

            } else { //merge output segment is empty just delete everything
                metadata.delete(
                        handle.sources
                );
                View<T> mergedView = currentView.delete(sources);
                swapView(mergedView);
                out.delete();
            }

            System.err.println("Completing " + handle);
            handle.view.close(); //release merge handle view
            compacting.remove(handle);

        } catch (Exception e) {
            //TODO add logging
            //TODO fail execution and mark handle ?
            System.err.println("FAILED TO MERGE");
            e.printStackTrace();
        }
    }

    private boolean eligibleForCompaction(View<T> view, T segment) {
        boolean alreadyCompacting = compacting.stream()
                .map(MergeHandle::sources)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet())
                .contains(segment);

        return !alreadyCompacting && !view.head().equals(segment);

    }

    private static <T extends SegmentFile> List<T> levelSegments(View<T> view, int level) {
        try (view) {
            return view.stream()
                    .filter(l -> segmentId(l).level() == level)
                    .collect(Collectors.toList());
        }
    }

    private static <T extends SegmentFile> long maxLevel(View<T> view) {
        try (view) {
            return view.stream()
                    .map(DirectoryUtils::segmentId)
                    .mapToInt(SegmentId::level)
                    .max()
                    .orElse(0);
        }
    }

    private long nextIdx(View<T> view, int level) {
        //do not change, other methods are iterating the view stream, it needs to be closed
        List<T> levelSegments = levelSegments(view, level);
        long mergingMax = compacting.stream()
                .map(MergeHandle::replacement)
                .map(DirectoryUtils::segmentId)
                .filter(segId -> segId.level() == level)
                .mapToLong(SegmentId::idx)
                .max()
                .orElse(-1);

        long currentSegmentsMax = levelSegments
                .stream()
                .map(DirectoryUtils::segmentId)
                .mapToLong(SegmentId::idx)
                .max()
                .orElse(-1);

        return max(mergingMax, currentSegmentsMax) + 1;

    }

    public View<T> view() {
        return viewRef.get().acquire();
    }

    @Override
    public synchronized void close() {
        this.viewRef.getAndSet(new View<>()).close();
    }

    public void delete() {
        try (var view = viewRef.getAndSet(new View<>())) {
            view.deleteAll();
        }
    }

}
