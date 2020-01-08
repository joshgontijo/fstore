package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class View {

    private static final Logger log = LoggerFactory.getLogger(View.class);
    private static final String EXT = ".log";

    private volatile List<IndexedSegment> segments = new CopyOnWriteArrayList<>();
    private volatile IndexedSegment head;

    private final AtomicLong nextSegmentIdx = new AtomicLong();
    private final AtomicLong entries = new AtomicLong();
    private final File root;
    private final int indexSize;
    private final BiFunction<File, Index, IndexedSegment> segmentFactory;
    private final BiFunction<File, Integer, Index> indexFactory;
    public static final int LEVEL_DIGITS = 3;
    public static final int SEG_IDX_DIGITS = (int) (Math.log10(Long.MAX_VALUE) + 1);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    View(File root, int indexSize, BiFunction<File, Index, IndexedSegment> segmentFactory, BiFunction<File, Integer, Index> indexFactory) throws IOException {
        this.root = root;
        this.indexSize = indexSize;
        this.segmentFactory = segmentFactory;
        this.indexFactory = indexFactory;

        List<IndexedSegment> segments = Files.list(root.toPath())
                .map(Path::toFile)
                .filter(f -> f.getName().endsWith(EXT))
                .map(this::open)
                .sorted(compareSegments())
                .collect(Collectors.toList());

        entries.set(segments.stream().mapToLong(IndexedSegment::entries).sum());

        if (!segments.isEmpty()) {
            IndexedSegment head = segments.get(segments.size() - 1);
            head.reindex(indexFactory);
            head.forceRoll();

            nextSegmentIdx.set(head.segmentId() + 1);
        }
        IndexedSegment head = createHead();
        this.head = head;
        this.segments.add(head);
    }

    private IndexedSegment createHead() {
        return newSegment(0, indexSize);
    }

    private static Comparator<IndexedSegment> compareSegments() {
        return (o1, o2) -> {
            int levelDiff = o2.level() - o1.level();
            if (levelDiff == 0) {
                int createdDiff = Long.compare(o1.segmentId(), o2.segmentId());
                if (createdDiff != 0)
                    return createdDiff;
            }
            return levelDiff;
        };
    }

    IndexedSegment head() {
        return head;
    }

    public long entries() {
        return entries.get() + head().entries();
    }

    public IndexedSegment newSegment(int level, long indexSize) {
        if (indexSize > Index.MAX_SIZE) {
            throw new IllegalArgumentException("Index cannot be greater than " + Index.MAX_SIZE);
        }
        long nextSegIdx = nextSegmentIdx.getAndIncrement();
        File segmentFile = segmentFile(nextSegIdx, level);
        File indexFile = indexFile(segmentFile);
        Index index = indexFactory.apply(indexFile, (int) indexSize);
        return segmentFactory.apply(segmentFile, index);
    }

    private IndexedSegment open(File segmentFile) {
        try {
            File indexFile = indexFile(segmentFile);
            Index index = indexFactory.apply(indexFile, indexSize);
            return segmentFactory.apply(segmentFile, index);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment " + segmentFile.getName(), e);
        }
    }

    private File segmentFile(long segmentIdx, int level) {
        String name = format("%0" + SEG_IDX_DIGITS + "d", segmentIdx) + "-" + format("%0" + LEVEL_DIGITS + "d", level) + EXT;
        return new File(root, name);
    }

    public static long segmentIdx(String fileName) {
        String name = nameWithoutExt(fileName);
        return Long.parseLong(name.split("-")[0]);
    }

    public static int levelOf(String fileName) {
        String name = nameWithoutExt(fileName);
        return Integer.parseInt(name.split("-")[1]);
    }

    static File indexFile(File segmentFile) {
        String name = nameWithoutExt(segmentFile.getName());
        File dir = segmentFile.toPath().getParent().toFile();
        return new File(dir, name + ".index");
    }

    private static String nameWithoutExt(String fileName) {
        return fileName.split("\\.")[0];
    }

    IndexedSegment roll() throws IOException {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            long start = System.currentTimeMillis();
            log.info("Rolling segment {}", head);
            head.roll();
            entries.addAndGet(head.entries());
            IndexedSegment newHead = createHead();

            segments.add(newHead);
            head = newHead;
            log.info("Segment {} rolled in {}ms", head, System.currentTimeMillis() - start);
            return newHead;
        } finally {
            lock.unlock();
        }
    }

    public void close() throws IOException {
        for (IndexedSegment segment : segments) {
            log.info("Closing segment {}", segment);
            segment.close();
        }
    }

    //----------------------------------

    private void appendSegment(IndexedSegment newHead) {

    }

    public <R> R apply(Direction direction, Function<List<IndexedSegment>, R> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<IndexedSegment> segs = getSegments(direction);
            return function.apply(segs);
        } finally {
            lock.unlock();
        }
    }

    public void lock(Runnable runnable) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    public void acquire(Direction direction, Consumer<List<IndexedSegment>> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<IndexedSegment> segs = getSegments(direction);
            function.accept(segs);
        } finally {
            lock.unlock();
        }
    }

    public <R> R apply(int level, Function<List<IndexedSegment>, R> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<IndexedSegment> segs = getSegments(level);
            return function.apply(segs);
        } finally {
            lock.unlock();
        }
    }

    List<IndexedSegment> getSegments(int level) {
        return segments.stream().filter(seg -> seg.level() == level).collect(Collectors.toList());
    }

    List<IndexedSegment> getSegments(Direction direction) {
        ArrayList<IndexedSegment> copy = new ArrayList<>(segments);
        if (Direction.BACKWARD.equals(direction)) {
            Collections.reverse(copy);
        }
        return copy;
    }

    public void remove(List<IndexedSegment> segments) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            validateDeletion(segments);
            List<IndexedSegment> copy = new ArrayList<>(this.segments);
            for (IndexedSegment seg : segments) {
                copy.remove(seg);
            }
            this.segments = copy;
        } finally {
            lock.unlock();
        }
    }

    public void merge(List<IndexedSegment> sources, IndexedSegment merged) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            if (sources.isEmpty() || merged == null) {
                return;
            }

            List<IndexedSegment> copy = new ArrayList<>(this.segments);
            validateDeletion(sources);

            IndexedSegment first = sources.get(0);
            int level = first.level(); //safe to assume that there is a segment and all of them are the same level
            int nextLevel = level + 1;
            int firstIdx = copy.indexOf(first);
            copy.set(firstIdx, merged);
            for (int i = 1; i < sources.size(); i++) {
                copy.remove(firstIdx + 1);
            }
            //TODO if the program crash after here, then there could be multiple READ_ONLY sources with same data
            //ideally target would have the sources in their header to flag where they're were created from.
            //on startup read all the source from the header and if there's an existing segment, then delete, either the sources
            //or the target segment
            merged.roll();
            this.segments = copy;
        } catch (IOException e) {
            throw new RuntimeIOException("Error while merging files", e);
        } finally {
            lock.unlock();
        }
    }

    private void validateDeletion(List<IndexedSegment> segments) {
        int latestIndex = -1;
        for (IndexedSegment seg : segments) {
            int i = segments.indexOf(seg);
            if (i < 0) {
                throw new IllegalStateException("Segment not found: " + seg.name());
            }
            if (latestIndex >= 0 && latestIndex + 1 != i) {
                throw new IllegalArgumentException("Segments to be deleted must be contiguous");
            }
            latestIndex = i;
        }
    }
}