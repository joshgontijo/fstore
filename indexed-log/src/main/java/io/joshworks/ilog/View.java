package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.joshworks.ilog.LogUtil.EXT;
import static io.joshworks.ilog.LogUtil.compareSegments;
import static io.joshworks.ilog.LogUtil.indexFile;
import static io.joshworks.ilog.LogUtil.segmentFile;

public class View {

    private static final Logger log = LoggerFactory.getLogger(View.class);

    private volatile List<Segment> segments = new CopyOnWriteArrayList<>();
    private final Set<Segment> compacting = new CopyOnWriteArraySet<>();

    private volatile Segment head;

    private final AtomicLong entries = new AtomicLong();
    private final File root;
    private final BufferPool pool;
    private final long maxLogSize;
    private final long maxLogEntries;
    private final SegmentFactory<?> segmentFactory;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    View(File root, BufferPool pool, long maxLogSize, long maxLogEntries, SegmentFactory<?> segmentFactory) {
        this.root = root;
        this.pool = pool;
        this.maxLogSize = maxLogSize;
        this.maxLogEntries = maxLogEntries;
        this.segmentFactory = segmentFactory;

        try {
            List<Segment> segments = Files.list(root.toPath())
                    .map(Path::toFile)
                    .filter(f -> f.getName().endsWith(EXT))
                    .map(this::open)
                    .sorted(compareSegments())
                    .collect(Collectors.toList());

            if (!segments.isEmpty()) {
                Segment head = segments.get(segments.size() - 1);
                head.restore();
                head.forceRoll();
            }

            entries.set(segments.stream().mapToLong(Segment::entries).sum());

            Segment head = createHead();
            this.head = head;
            this.segments.add(head);

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to load segments", e);
        }
    }

    private Segment createHead() {
        return newSegment(0, maxLogSize, maxLogEntries);
    }


    Segment head() {
        return head;
    }

    void delete() {
        lock(() -> {
            for (Segment segment : segments) {
                segment.delete();
            }
            segments.clear();
        });
    }

    public long entries() {
        return entries.get() + head().entries();
    }

    public Segment newSegment(int level, long maxLogSize, long maxEntries) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long nextSegIdx = nextSegmentIdx(segments, level);
            File segmentFile = segmentFile(root, nextSegIdx, level);
            for (Segment segment : segments) {
                if (segment.name().equals(segmentFile.getName())) {
                    throw new IllegalStateException("Duplicate segment name");
                }
            }
            return segmentFactory.create(segmentFile, pool, maxLogSize, maxEntries);
        } finally {
            lock.unlock();
        }
    }

    private Segment open(File segmentFile) {
        try {
            File indexFile = indexFile(segmentFile);
            return segmentFactory.create(indexFile, pool, maxLogSize, maxLogEntries);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment " + segmentFile.getName(), e);
        }
    }

    Segment roll() {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            long start = System.currentTimeMillis();
            log.info("Rolling segment {}", head);
            head.roll();
            log.info("Segment {} rolled in {}ms", head, System.currentTimeMillis() - start);
            entries.addAndGet(head.entries());
            Segment newHead = createHead();

            segments.add(newHead);
            head = newHead;
            return newHead;
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        for (Segment segment : segments) {
            log.info("Closing segment {}", segment);
            segment.close();
        }
    }

    //----------------------------------

    public <T extends Segment, R> R apply(Direction direction, Function<List<T>, R> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<T> segs = getSegments(direction);
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

    public <T extends Segment> void acquire(Direction direction, Consumer<List<T>> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<T> segs = getSegments(direction);
            function.accept(segs);
        } finally {
            lock.unlock();
        }
    }

    public <T extends Segment, R> R apply(int level, Function<List<T>, R> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<T> segs = getSegments(level);
            return function.apply(segs);
        } finally {
            lock.unlock();
        }
    }

    <T extends Segment> List<T> getSegments(int level) {
        List<T> copy = new ArrayList<>();
        for (Segment segment : segments) {
            if (segment.level() == level) {
                copy.add((T) segment);
            }
        }
        return copy;
    }

    <T extends Segment> List<T> getSegments(Direction direction) {
        List<Segment> copy = new ArrayList<>(segments);
        if (Direction.BACKWARD.equals(direction)) {
            Collections.reverse(copy);
        }
        return (List<T>) copy;
    }

    public void remove(List<Segment> segments) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            validateDeletion(segments);
            List<Segment> copy = new ArrayList<>(this.segments);
            for (Segment seg : segments) {
                copy.remove(seg);
            }
            this.segments = copy;

            for (Segment seg : segments) {
                seg.delete();
            }

        } finally {
            lock.unlock();
        }
    }

    public void merge(List<Segment> sources, Segment output) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            if (sources.isEmpty() || output == null) {
                return;
            }

            List<Segment> copy = new ArrayList<>(this.segments);
            validateDeletion(sources);

            Segment first = sources.get(0);
            int level = first.level(); //safe to assume that there is a segment and all of them are the same level
            int nextLevel = level + 1;
            int firstIdx = copy.indexOf(first);
            copy.set(firstIdx, output);
            for (int i = 1; i < sources.size(); i++) {
                copy.remove(firstIdx + 1);
            }
            //TODO if the program crash after here, then there could be multiple READ_ONLY sources with same data
            //ideally target would have the sources in their header to flag where they're were created from.
            //on startup read all the source from the header and if there's an existing segment, then delete, either the sources
            //or the target segment
            output.roll();
            this.segments = copy;

            for (Segment source : sources) {
                source.delete();
            }

        } finally {
            lock.unlock();
        }
    }

    private void validateDeletion(List<Segment> segments) {
        int latestIndex = -1;
        for (Segment seg : segments) {
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

    private static long nextSegmentIdx(List<Segment> segments, int level) {
        return segments.stream()
                .filter(seg -> seg.level() == level)
                .mapToLong(Segment::segmentIdx)
                .max()
                .orElse(-1) + 1;
    }

    public boolean isCompacting(Segment segment) {
        return compacting.contains(segment);
    }

    public void addToCompacting(List<Segment> toBeCompacted) {
        compacting.addAll(toBeCompacted);
    }

    public void removeFromCompacting(List<Segment> toBeCompacted) {
        compacting.removeAll(toBeCompacted);
    }
}