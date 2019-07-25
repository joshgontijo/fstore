package io.joshworks.fstore.log.appender.level;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.header.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

//LEVEL0 [CURRENT_SEGMENT]
//LEVEL1 [SEG1][SEG2]
//LEVEL2 [SEG3][SEG4]
//LEVEL3 ...
public class Levels<T> {

    private volatile List<Log<T>> segments = new CopyOnWriteArrayList<>();
    private volatile Log<T> current;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private Levels(List<Log<T>> segments) {
        this.segments.addAll(segments);
        this.segments.sort((o1, o2) -> {
            int levelDiff = o2.level() - o1.level();
            if (levelDiff == 0) {
                int createdDiff = Long.compare(o1.created(), o2.created());
                if (createdDiff != 0)
                    return createdDiff;
            }
            return levelDiff;
        });

        if (this.segments.stream().noneMatch(seg -> seg.level() == 0)) {
            throw new IllegalStateException("Level zero must be present");
        }

        long logHeadCount = this.segments.stream().filter(seg -> Type.LOG_HEAD.equals(seg.type())).count();
        if (logHeadCount != 1) {
            throw new IllegalStateException("Expected single " + Type.LOG_HEAD + " segment, found " + logHeadCount);
        }
        this.current = this.segments.stream()
                .filter(seg -> Type.LOG_HEAD.equals(seg.type()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No " + Type.LOG_HEAD + " found"));
    }


    public <R> R apply(Direction direction, Function<List<Log<T>>, R> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<Log<T>> segs = getSegments(direction);
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

    public void acquire(Direction direction, Consumer<List<Log<T>>> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<Log<T>> segs = getSegments(direction);
            function.accept(segs);
        } finally {
            lock.unlock();
        }
    }

    public <R> R apply(int level, Function<List<Log<T>>, R> function) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            List<Log<T>> segs = getSegments(level);
            return function.apply(segs);
        } finally {
            lock.unlock();
        }
    }

    //not thread safe to use reference
    public Log<T> current() {
        return current;
    }

    public Log<T> get(int segmentIdx) {
        Lock lock = this.rwLock.readLock();
        lock.lock();
        try {
            int size = segments.size();
            if (segmentIdx < 0 || segmentIdx >= size) {
                throw new IllegalArgumentException("Invalid segment idx: " + segmentIdx + ", size: " + size);
            }
            return segments.get(segmentIdx);
        } finally {
            lock.unlock();
        }
    }

    public int depth() {
        return apply(Direction.FORWARD, segs -> segs.stream().mapToInt(Log::level).max().orElse(0));
    }

    public static <T> Levels<T> create(List<Log<T>> segments) {
        return new Levels<>(segments);
    }

    public void appendSegment(Log<T> newHead) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            if (newHead.level() != 0) {
                throw new IllegalArgumentException("New segment must be level zero");
            }
            int size = segments.size();
            if (size == 0) {
                segments.add(newHead);
                return;
            }
            if (!current.readOnly()) {
                throw new IllegalStateException("Segment must be marked as read only after rolling");
            }
            segments.add(newHead);
            current = newHead;
        } finally {
            lock.unlock();
        }
    }

    public int numSegments() {
        return segments.size();
    }

    public void remove(List<Log<T>> segments) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            validateDeletion(segments);
            List<Log<T>> copy = new ArrayList<>(this.segments);
            for (Log<T> seg : segments) {
                copy.remove(seg);
            }
            this.segments = copy;
        } finally {
            lock.unlock();
        }
    }

    public void merge(List<Log<T>> sources, Log<T> merged) {
        Lock lock = this.rwLock.writeLock();
        lock.lock();
        try {
            if (sources.isEmpty() || merged == null) {
                return;
            }

            List<Log<T>> copy = new ArrayList<>(this.segments);
            validateDeletion(sources);

            Log<T> first = sources.get(0);
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
            merged.roll(nextLevel, true);
            this.segments = copy;
        } finally {
            lock.unlock();
        }
    }

//    public synchronized void merge(List<Log<T>> segments, Log<T> merged) {
//        if (segments.isEmpty() || merged == null) {
//            return;
//        }
//
//        List<Log<T>> copy = new ArrayList<>(this.segments);
//
//        int latestIndex = -1;
//        for (Log<T> seg : segments) {
//            int i = copy.indexOf(seg);
//            if (i < 0) {
//                throw new IllegalStateException("Segment not found: " + seg.name());
//            }
//            if (latestIndex >= 0 && latestIndex + 1 != i) {
//                throw new IllegalArgumentException("Segments to be deleted must be contiguous");
//            }
//            latestIndex = i;
//        }
//
//        Log<T> first = segments.get(0);
//        int level = first.level(); //safe to assume that there is a segment and all of them are the same level
//        int nextLevel = level + 1;
//        int firstIdx = copy.indexOf(first);
//        copy.set(firstIdx, merged);
//        for (int i = 1; i < segments.size(); i++) {
//            copy.remove(firstIdx + 1);
//        }
//        //TODO if the program cash after here, then there could be multiple READ_ONLY segments with same data
//        //ideally segments would have the source segments in their header to flag where they're were created from.
//        //on startup read all the source segments from the header and if there's an existing segment, then delete, either the sources
//        //or the target segment
//        merged.roll(nextLevel);
//        this.segments = copy;
//
//    }

    private void validateDeletion(List<Log<T>> segments) {
        int latestIndex = -1;
        for (Log<T> seg : segments) {
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

    List<Log<T>> getSegments(int level) {
        return segments.stream().filter(seg -> seg.level() == level).collect(Collectors.toList());
    }

    List<Log<T>> getSegments(Direction direction) {
        ArrayList<Log<T>> copy = new ArrayList<>(segments);
        if (Direction.BACKWARD.equals(direction)) {
            Collections.reverse(copy);
        }
        return copy;
    }

    @Override
    public String toString() {
        return "Levels{" + "depth=" + depth() + ", items=" + Arrays.toString(segments.toArray()) + '}';
    }
}