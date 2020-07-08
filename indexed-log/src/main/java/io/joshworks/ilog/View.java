package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.record.RecordPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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

public class View<T extends IndexedSegment> {

    private static final Logger log = LoggerFactory.getLogger(View.class);


    public volatile List<T> segments = new CopyOnWriteArrayList<>();
    private volatile T head;

    private final AtomicLong nextSegmentIdx = new AtomicLong();
    private final AtomicLong entries = new AtomicLong();
    private final File root;
    private final RecordPool pool;
    private final long indexEntries;
    private final SegmentFactory<T> segmentFactory;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    //TODO remove BufferPool
    View(File root, RecordPool pool, long indexEntries, SegmentFactory<T> segmentFactory) throws IOException {
        this.root = root;
        this.pool = pool;
        this.indexEntries = indexEntries;
        this.segmentFactory = segmentFactory;

        List<T> segments = Files.list(root.toPath())
                .map(Path::toFile)
                .filter(f -> f.getName().endsWith(EXT))
                .map(this::open)
                .sorted(compareSegments())
                .collect(Collectors.toList());

        entries.set(segments.stream().mapToLong(T::entries).sum());

        if (!segments.isEmpty()) {
            T head = segments.get(segments.size() - 1);
            head.reindex();
            head.forceRoll();

            nextSegmentIdx.set(head.segmentId() + 1);
        }
        T head = createHead();
        this.head = head;
        this.segments.add(head);
    }

    private T createHead() {
        return newSegment(0, indexEntries);
    }


    T head() {
        return head;
    }

    void delete() {
        lock(() -> {
            for (T segment : segments) {
                segment.delete();
            }
            segments.clear();
        });
    }

    public long entries() {
        return entries.get() + head().entries();
    }

    public T newSegment(int level, long indexEntries) {
        long nextSegIdx = nextSegmentIdx.getAndIncrement();
        File segmentFile = segmentFile(root, nextSegIdx, level);
        return segmentFactory.create(segmentFile, indexEntries, pool);
    }

    private T open(File segmentFile) {
        try {
            File indexFile = indexFile(segmentFile);
            return segmentFactory.create(indexFile, indexEntries, pool);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment " + segmentFile.getName(), e);
        }
    }


    T roll() throws IOException {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            long start = System.currentTimeMillis();
            log.info("Rolling segment {}", head);
            head.roll();
            log.info("Segment {} rolled in {}ms", head, System.currentTimeMillis() - start);
            entries.addAndGet(head.entries());
            T newHead = createHead();

            segments.add(newHead);
            head = newHead;
            return newHead;
        } finally {
            lock.unlock();
        }
    }

    public void close() throws IOException {
        for (T segment : segments) {
            log.info("Closing segment {}", segment);
            segment.close();
        }
    }

    //----------------------------------

    public <R> R apply(Direction direction, Function<List<T>, R> function) {
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

    public void acquire(Direction direction, Consumer<List<T>> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<T> segs = getSegments(direction);
            function.accept(segs);
        } finally {
            lock.unlock();
        }
    }

    public <R> R apply(int level, Function<List<T>, R> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<T> segs = getSegments(level);
            return function.apply(segs);
        } finally {
            lock.unlock();
        }
    }

    List<T> getSegments(int level) {
        return segments.stream().filter(seg -> seg.level() == level).collect(Collectors.toList());
    }

    List<T> getSegments(Direction direction) {
        ArrayList<T> copy = new ArrayList<>(segments);
        if (Direction.BACKWARD.equals(direction)) {
            Collections.reverse(copy);
        }
        return copy;
    }

    public void remove(List<T> segments) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            validateDeletion(segments);
            List<T> copy = new ArrayList<>(this.segments);
            for (T seg : segments) {
                copy.remove(seg);
            }
            this.segments = copy;

            for (T seg : segments) {
                seg.delete();
            }

        } finally {
            lock.unlock();
        }
    }

    public void merge(List<T> sources, T merged) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            if (sources.isEmpty() || merged == null) {
                return;
            }

            List<T> copy = new ArrayList<>(this.segments);
            validateDeletion(sources);

            T first = sources.get(0);
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

            for (T source : sources) {
                source.delete();
            }

        } finally {
            lock.unlock();
        }
    }

    private void validateDeletion(List<T> segments) {
        int latestIndex = -1;
        for (T seg : segments) {
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