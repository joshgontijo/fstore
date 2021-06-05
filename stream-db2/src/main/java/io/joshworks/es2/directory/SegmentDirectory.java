package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.iterators.CloseableIterator;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.joshworks.es2.directory.DirectoryUtils.deleteAllWithExtension;
import static io.joshworks.es2.directory.DirectoryUtils.initDirectory;
import static io.joshworks.es2.directory.DirectoryUtils.level;
import static io.joshworks.es2.directory.DirectoryUtils.segmentFileName;
import static io.joshworks.es2.directory.DirectoryUtils.segmentIdx;
import static java.lang.Math.max;

/**
 * Not Thread safe, synchronization must be done outside
 * Mainly because EventStore needs to sync on both Log and Index a the same time, adding here would just be unecessary overhead
 * head() does not need sync as it does not change
 */
public class SegmentDirectory<T extends SegmentFile> implements Iterable<T>, Closeable {

    private static final String MERGE_EXT = "tmp";

    private final TreeSet<T> segments = new TreeSet<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
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
        deleteAllWithExtension(root, MERGE_EXT);
    }

    public File newHead() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long segIdx = 0;
            if (!segments.isEmpty()) {
                T currentHead = segments.first();
                int headLevel = level(currentHead);
                if (headLevel == 0) {
                    segIdx = segmentIdx(currentHead) + 1;
                }
            }
            return createFile(0, segIdx, extension);
        } finally {
            lock.unlock();
        }
    }

    private File createFile(int level, long segIdx, String ext) {
        String name = segmentFileName(segIdx, level, ext);
        return new File(root, name);
    }

    //modifications must be synchronized
    public void append(T newSegmentHead) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            if (!segments.isEmpty() && segments.first().compareTo(newSegmentHead) <= 0) {
                throw new IllegalStateException("New segment won't be new head");
            }
            segments.add(newSegmentHead);
            assert segments.first().equals(newSegmentHead);
        } finally {
            lock.unlock();
        }
    }

    public void loadSegments() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            Files.list(root.toPath())
                    .map(Path::toFile)
                    .filter(this::matchExtension)
                    .map(supplier)
                    .forEach(segments::add);

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to load segments", e);
        } finally {
            lock.unlock();
        }
    }

    //TODO move parameters to constructor
    public CompletableFuture<Void> compact(int minItems, int maxItems) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            List<CompletableFuture<Void>> tasks = new ArrayList<>();
            for (int level = 0; level <= topLevel(); level++) {

                List<T> levelSegments = eligibleMergeSegments(level);

                int nextLevel = level + 1;
                do {
                    int items = max(maxItems, levelSegments.size());
                    List<T> sublist = new ArrayList<>(levelSegments.subList(0, items));
                    levelSegments.removeAll(sublist);

                    File replacementFile = createFile(nextLevel, topIdx(nextLevel), MERGE_EXT);
                    MergeHandle<T> handle = new MergeHandle<>(replacementFile, sublist);

                    merging.addAll(sublist);

                    tasks.add(submit(handle));

                } while (levelSegments.size() >= minItems);
            }

            return CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new));

        } finally {
            lock.unlock();
        }
    }

    private CompletableFuture<Void> submit(MergeHandle<T> handle) {
        return CompletableFuture.runAsync(() -> safeCompact(handle), executor)
                .thenRun(() -> completeMerge(handle));
    }

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
            handle.sources.forEach(merging::remove);
            //TODO logging
            e.printStackTrace();
        }
    }

    private synchronized void completeMerge(MergeHandle<T> handle) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            File file = handle.replacement();
            assert file.getName().endsWith(MERGE_EXT);

            String newFileName = file.getName().replaceAll("\\." + MERGE_EXT, "." + extension);
            Path path = file.toPath();
            File renamed = Files.move(path, path.getParent().resolve(newFileName)).toFile();

            handle.sources().forEach(seg -> {
                boolean v1 = segments.remove(seg);
                boolean v2 = merging.remove(seg);
                assert v1;
                assert v2;
                seg.delete();
            });
            if (renamed.length() == 0) {
                Files.delete(renamed.toPath());
            } else {
                segments.add(supplier.apply(renamed));
            }

        } catch (Exception e) {
            //TODO add logging
            System.err.println("FAILED TO MERGE");
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private List<T> eligibleMergeSegments(int level) {
        var head = segments.first();
        return levelSegments(level)
                .filter(s -> !merging.contains(s))
                .filter(s -> !s.equals(head)) //head is used to keep track of log and sstable pairs, used for restoring
                .collect(Collectors.toList());
    }

    private Stream<T> levelSegments(int level) {
        return segments.stream().filter(l -> level(l) == level);
    }

    private long topLevel() {
        return segments.stream()
                .mapToInt(DirectoryUtils::level)
                .max()
                .orElse(0);
    }

    private long topIdx(int level) {
        return levelSegments(level)
                .mapToLong(DirectoryUtils::segmentIdx)
                .max()
                .orElse(0);
    }

    private boolean matchExtension(File file) {
        return file.getName().endsWith(extension);
    }

    @Override
    public void close() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            for (T segment : segments) {
                segment.close();
            }
            segments.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SegmentIterator iterator() {
        Lock lock = rwLock.readLock();
        lock.lock();
        return new SegmentIterator(lock, segments.iterator());
    }

    public SegmentIterator reverse() {
        Lock lock = rwLock.readLock();
        lock.lock();
        return new SegmentIterator(lock, segments.descendingIterator());
    }

    public void delete() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            for (T segment : segments) {
                segment.delete();
            }
            segments.clear();
            try {
                Files.delete(root.toPath());
            } catch (IOException e) {
                throw new RuntimeIOException("Failed to delete directory ", e);
            }
        } finally {
            lock.unlock();
        }
    }

    public class SegmentIterator implements CloseableIterator<T> {

        private final Lock lock;
        private final Iterator<T> delegate;

        private SegmentIterator(Lock lock, Iterator<T> delegate) {
            this.lock = lock;
            this.delegate = delegate;
        }

        @Override
        public void close() {
            lock.unlock();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public T next() {
            return delegate.next();
        }
    }

}
