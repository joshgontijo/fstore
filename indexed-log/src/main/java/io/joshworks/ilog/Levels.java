//package io.joshworks.ilog;
//
//import io.joshworks.fstore.core.RuntimeIOException;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.List;
//import java.util.concurrent.CopyOnWriteArrayList;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReadWriteLock;
//import java.util.concurrent.locks.ReentrantReadWriteLock;
//import java.util.function.Consumer;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
////LEVEL0 [CURRENT_SEGMENT]
////LEVEL1 [SEG1][SEG2]
////LEVEL2 [SEG3][SEG4]
////LEVEL3 ...
//public class Levels {
//
//    private volatile List<IndexedSegment> segments = new CopyOnWriteArrayList<>();
//
//    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
//
//    private Levels(List<IndexedSegment> segments) {
//        this.segments.addAll(segments);
//        this.segments.sort(compareSegments());
//    }
//
//
//
//    public <R> R apply(Direction direction, Function<List<IndexedSegment>, R> function) {
//        Lock lock = this.rwLock.readLock();
//        lock.lock();
//        try {
//            List<IndexedSegment> segs = getSegments(direction);
//            return function.apply(segs);
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    public void lock(Runnable runnable) {
//        Lock lock = this.rwLock.writeLock();
//        lock.lock();
//        try {
//            runnable.run();
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    public void acquire(Direction direction, Consumer<List<IndexedSegment>> function) {
//        Lock lock = this.rwLock.readLock();
//        lock.lock();
//        try {
//            List<IndexedSegment> segs = getSegments(direction);
//            function.accept(segs);
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    public <R> R apply(int level, Function<List<IndexedSegment>, R> function) {
//        Lock lock = this.rwLock.readLock();
//        lock.lock();
//        try {
//            List<IndexedSegment> segs = getSegments(level);
//            return function.apply(segs);
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    public IndexedSegment get(int segmentIdx) {
//        Lock lock = this.rwLock.readLock();
//        lock.lock();
//        try {
//            int size = segments.size();
//            if (segmentIdx < 0 || segmentIdx >= size) {
//                throw new IllegalArgumentException("Invalid segment idx: " + segmentIdx + ", size: " + size);
//            }
//            return segments.get(segmentIdx);
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    public static Levels create(List<IndexedSegment> segments) {
//        return new Levels(segments);
//    }
//
//    public void appendSegment(IndexedSegment newHead) {
//        Lock lock = this.rwLock.writeLock();
//        lock.lock();
//        try {
//            if (newHead.level() != 0) {
//                throw new IllegalArgumentException("New segment must be level zero");
//            }
//            int size = segments.size();
//            if (size == 0) {
//                segments.add(newHead);
//                return;
//            }
//            if (!current.readOnly()) {
//                throw new IllegalStateException("Segment must be marked as read only after rolling");
//            }
//            segments.add(newHead);
//            current = newHead;
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    public int numSegments() {
//        return segments.size();
//    }
//
//    public void remove(List<IndexedSegment> segments) {
//        Lock lock = this.rwLock.writeLock();
//        lock.lock();
//        try {
//            validateDeletion(segments);
//            List<IndexedSegment> copy = new ArrayList<>(this.segments);
//            for (IndexedSegment seg : segments) {
//                copy.remove(seg);
//            }
//            this.segments = copy;
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    public void merge(List<IndexedSegment> sources, IndexedSegment merged) {
//        Lock lock = this.rwLock.writeLock();
//        lock.lock();
//        try {
//            if (sources.isEmpty() || merged == null) {
//                return;
//            }
//
//            List<IndexedSegment> copy = new ArrayList<>(this.segments);
//            validateDeletion(sources);
//
//            IndexedSegment first = sources.get(0);
//            int level = first.level(); //safe to assume that there is a segment and all of them are the same level
//            int nextLevel = level + 1;
//            int firstIdx = copy.indexOf(first);
//            copy.set(firstIdx, merged);
//            for (int i = 1; i < sources.size(); i++) {
//                copy.remove(firstIdx + 1);
//            }
//            //TODO if the program crash after here, then there could be multiple READ_ONLY sources with same data
//            //ideally target would have the sources in their header to flag where they're were created from.
//            //on startup read all the source from the header and if there's an existing segment, then delete, either the sources
//            //or the target segment
//            merged.roll();
//            this.segments = copy;
//        } catch (IOException e) {
//            throw new RuntimeIOException("Error while merging files", e);
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    private void validateDeletion(List<IndexedSegment> segments) {
//        int latestIndex = -1;
//        for (IndexedSegment seg : segments) {
//            int i = segments.indexOf(seg);
//            if (i < 0) {
//                throw new IllegalStateException("Segment not found: " + seg.name());
//            }
//            if (latestIndex >= 0 && latestIndex + 1 != i) {
//                throw new IllegalArgumentException("Segments to be deleted must be contiguous");
//            }
//            latestIndex = i;
//        }
//    }
//
//    List<IndexedSegment> getSegments(int level) {
//        return segments.stream().filter(seg -> seg.level() == level).collect(Collectors.toList());
//    }
//
//    List<IndexedSegment> getSegments(Direction direction) {
//        ArrayList<IndexedSegment> copy = new ArrayList<>(segments);
//        if (Direction.BACKWARD.equals(direction)) {
//            Collections.reverse(copy);
//        }
//        return copy;
//    }
//
//    @Override
//    public String toString() {
//        return "Levels{" + "items=" + Arrays.toString(segments.toArray()) + '}';
//    }
//}