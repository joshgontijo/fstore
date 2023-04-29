package io.joshworks.es2.directory;

import io.joshworks.es2.sstable.CompactionConfig;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import static java.lang.Math.min;

public class SegmentDirectory<T extends SegmentFile> implements Closeable {


    public static final String METADATA_EXT = "metadata";
    private final Logger logger;
    private final AtomicReference<View<T>> viewRef;
    private final Set<CompactionItem<T>> compacting = new HashSet<>();

    private final File root;
    private final Metadata<T> metadata;
    private final Function<File, T> segmentFn;
    private final String extension;
    private final ExecutorService executor;
    private final Compaction<T> compaction;

    public SegmentDirectory(File root,
                            Function<File, T> segmentFn,
                            String extension,
                            ExecutorService executor,
                            Compaction<T> compaction) {

        logger = LoggerFactory.getLogger(extension);

        this.root = root;
        this.segmentFn = segmentFn;
        this.extension = extension;
        this.executor = executor;
        this.compaction = compaction;
        this.metadata = new Metadata<>(new File(root, extension + "." + METADATA_EXT));

        var view = loadSegments(root, metadata, extension, segmentFn, logger);
        viewRef = new AtomicReference<>(view);
        initDirectory(root);

//        deleteAllWithExtension(root);
    }

    private static <T extends SegmentFile> View<T> loadSegments(File root, Metadata<T> metadata, String extension, Function<File, T> segmentFn, Logger logger) {
        try {
            List<T> segments = metadata.state()
                    .stream()
                    .map(segId -> segmentFileName(segId.idx(), segId.level(), extension))
                    .map(name -> new File(root, name))
                    .map(segmentFn)
                    .collect(Collectors.toList());

            for (T segment : segments) {
                logger.info("Loaded {}", segment);
            }

            return new View<>(segments, logger);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to load segments", e);
        }
    }

    private static <T extends SegmentFile> List<T> levelSegments(View<T> view, int level) {
        return view.stream()
                .filter(l -> segmentId(l).level() == level)
                .collect(Collectors.toList());
    }

    private static <T extends SegmentFile> long maxLevel(View<T> view) {
        return view.stream()
                .map(DirectoryUtils::segmentId)
                .mapToInt(SegmentId::level)
                .max()
                .orElse(0);
    }

    public synchronized File newHead() {
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
        assert !Files.exists(Path.of(name));
        File file = new File(root, name);
        if (!FileUtils.createIfNotExists(file)) {
            throw new RuntimeException("File Already exist + " + file.getAbsolutePath());
        }
        return file;
    }

    public synchronized void append(T newSegmentHead) {
        var currView = this.viewRef.get();
        if (!currView.isEmpty() && currView.head().compareTo(newSegmentHead) <= 0) {
            throw new IllegalStateException("Invalid segment head");
        }
        metadata.add(newSegmentHead);
        View<T> newView = currView.add(newSegmentHead);
        swapView(newView);
    }

    private void swapView(View<T> view) {
        viewRef.getAndSet(view).close();
    }

    //TODO move parameters to constructor
    public synchronized CompletableFuture<CompactionResult> compact(CompactionConfig config) {
        var view = view();

        var task = CompletableFuture.completedFuture(new CompactionResult());
        for (var level = 0; level <= maxLevel(view); level++) {
            var threshold = config.profileForLevel(level).compactionThreshold();

            T head = view.head();
            List<T> levelSegments = levelSegments(view, level)
                    .stream()
                    .filter(seg -> eligibleForCompaction(view, seg))
                    .filter(s -> !head.equals(s))
                    .collect(Collectors.toList());
            Collections.reverse(levelSegments); //reverse so we start from the oldest files first

            int nextLevel = level + 1;
            while (levelSegments.size() >= threshold) {
                int items = min(threshold, levelSegments.size());
                var sublist = new ArrayList<>(levelSegments.subList(0, items));
                levelSegments.removeAll(sublist);

                var replacementFile = createFile(nextLevel, nextIdx(nextLevel));
                var handle = new CompactionItem<>(view, replacementFile, sublist, level);

                System.err.println("Submitting " + handle);
                task = task.thenCombine(submit(handle), CompactionResult::merge);
                compacting.add(handle);
            }
        }

        //we need to release the view as no compaction will happen, and we acquired it above
        if (compacting.isEmpty()) {
            view.close();
        }

        return task;
    }

    private CompletableFuture<CompactionResult> submit(CompactionItem<T> handle) {
        return CompletableFuture.supplyAsync(() -> safeCompact(handle), executor)
                .thenApply(this::completeMerge);
    }

    //TODO handle error cases, view must always be closed afterwards
    private CompactionItem<T> safeCompact(CompactionItem<T> handle) {
        try {
            compaction.compact(handle);
            handle.success = true;
        } catch (Exception e) {

            FileUtils.deleteIfExists(handle.replacement());
            e.printStackTrace();
        }
        return handle;
    }

    private synchronized CompactionResult completeMerge(CompactionItem<T> handle) {
        System.out.println("Completing merge");

        var currentView = viewRef.get();
        try {
            if (!handle.success) {
                return new CompactionResult();
            }

            var result = new CompactionResult(handle);
            var outFile = handle.replacement();

            T out = segmentFn.apply(outFile);
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
            compacting.remove(handle);

            return result;
        } catch (Exception e) {
            //TODO add logging
            //TODO fail execution and mark handle ?
            System.err.println("FAILED TO MERGE");
            e.printStackTrace();
            throw new RuntimeException("TODO handle");
        } finally {
            //safeguard to prevent 'double free' of the same view
            //to dot reuse 'currentView' here as we need to get the latest from the viewRef
            if (handle.view != viewRef.get()) {
                handle.view.close();
            }
        }
    }

    private boolean eligibleForCompaction(View<T> view, T segment) {
        boolean alreadyCompacting = compacting.stream()
                .map(CompactionItem::sources)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet())
                .contains(segment);

        return !alreadyCompacting && !view.head().equals(segment);

    }

    //FIXME io.joshworks.fstore.core.RuntimeIOException: Failed to create segment 0000000001-0000000000000000000.sst
    //Submitting [0000000000-0000000000000000000, 0000000000-0000000000000000001, 0000000000-0000000000000000002] -> 0000000001-0000000000000000000.sst
    //...
    //Completing [0000000001-0000000000000000000, 0000000001-0000000000000000001, 0000000001-0000000000000000002] -> 0000000002-0000000000000000000.sst
    //Submitting [0000000000-0000000000000000009, 0000000000-0000000000000000010, 0000000000-0000000000000000011] -> 0000000001-0000000000000000000.sst
    //level ends up empty, idx start again from zero, compaction tries to create a file, but the old still exist
    //TODO - ISSUE DESCRIBED ABOVE SEEMS FIXED
    long nextIdx(int level) {
        try {
            return Files.list(this.root.toPath())
                    .map(Path::getFileName)
                    .map(Path::toFile)
                    .filter(fileName -> fileName.getName().endsWith(extension))
                    .map(DirectoryUtils::segmentId)
                    .filter(s -> s.level() == level)
                    .mapToLong(SegmentId::idx)
                    .max()
                    .orElse(-1) + 1;

        } catch (IOException e) {
            throw new RuntimeIOException("Failed to determine next segment idx", e);
        }
    }

    public View<T> view() {
        View<T> view;
        while ((view = viewRef.get().acquire()) == null) {
            Thread.onSpinWait();
        }
        return view;
    }

    @Override
    public synchronized void close() {
        var view = this.viewRef.getAndSet(new View<>(this.logger));
        for (T segment : view) {
            segment.close();
        }
        view.close();
        this.metadata.close();
    }

    public synchronized void delete() {
        try (var view = viewRef.getAndSet(new View<>(this.logger))) {
            view.deleteAll();
        }
    }

}
