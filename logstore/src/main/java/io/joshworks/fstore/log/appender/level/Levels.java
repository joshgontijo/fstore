package io.joshworks.fstore.log.appender.level;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

//LEVEL0 [CURRENT_SEGMENT]
//LEVEL1 [SEG1][SEG2]
//LEVEL2 [SEG3][SEG4]
//LEVEL3 ...
public class Levels<T> {

    private final int maxItemsPerLevel;
    private volatile List<Log<T>> segments = new CopyOnWriteArrayList<>();
    private volatile Log<T> current;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Levels(int maxItemsPerLevel, List<Log<T>> segments) {
        this.maxItemsPerLevel = maxItemsPerLevel;

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
        this.current = segments.get(segments.size() - 1);
    }

    public List<Log<T>> segments(int level) {
        return segments.stream().filter(seg -> seg.level() == level).collect(Collectors.toList());
    }

    public Log<T> get(int segmentIdx) {
        return segments.get(segmentIdx);
    }

    public int depth() {
        return segments.stream().mapToInt(Log::level).max().orElse(0);
    }

    public static synchronized <T> Levels<T> create(int maxItemsPerLevel, List<Log<T>> segments) {
        return new Levels<>(maxItemsPerLevel, segments);
    }

    public void appendSegment(Log<T> segment) {
        if (segment.level() != 0) {
            throw new IllegalArgumentException("New segment must be level zero");
        }
        int size = segments.size();
        if (size == 0) {
            segments.add(segment);
            return;
        }
        Log<T> prevHead = segments.get(size - 1);

        prevHead.roll(1);
        if (!prevHead.readOnly()) {
            throw new IllegalStateException("Segment must be marked as read only after rolling");
        }
        segments.add(segment);
        current = segment;
    }

    public int numSegments() {
        return segments.size();
    }

    //TODO testing atomic acquiring of readers without the risk of closing the segment in between
    public synchronized List<LogIterator<T>> select(Direction direction, Function<Log<T>, LogIterator<T>> mapper) {
        return Iterators.closeableStream(segments(direction)).map(mapper).collect(Collectors.toList());
    }

    public synchronized int size(int level) {
        if (level < 0) {
            throw new IllegalArgumentException("Level must be at least zero");
        }
        if (level >= depth()) {
            return 0;
        }
        return segments(level).size();
    }

    public int compactionThreshold() {
        return maxItemsPerLevel;
    }

    public synchronized void remove(List<Log<T>> segments) {

        List<Log<T>> copy = new ArrayList<>(this.segments);

        int latestIndex = -1;
        for (Log<T> seg : segments) {
            int i = copy.indexOf(seg);
            if (i < 0) {
                throw new IllegalStateException("Segment not found: " + seg.name());
            }
            if (latestIndex >= 0 && latestIndex + 1 != i) {
                throw new IllegalArgumentException("Segments to be deleted must be contiguous");
            }
            latestIndex = i;
        }
        for (Log<T> seg : segments) {
            copy.remove(seg);
        }
        this.segments = copy;
    }

    public synchronized void merge(List<Log<T>> segments, Log<T> merged) {
        if (segments.isEmpty() || merged == null) {
            return;
        }

        List<Log<T>> copy = new ArrayList<>(this.segments);

        int latestIndex = -1;
        for (Log<T> seg : segments) {
            int i = copy.indexOf(seg);
            if (i < 0) {
                throw new IllegalStateException("Segment not found: " + seg.name());
            }
            if (latestIndex >= 0 && latestIndex + 1 != i) {
                throw new IllegalArgumentException("Segments to be deleted must be contiguous");
            }
            latestIndex = i;
        }

        int firstIdx = copy.indexOf(segments.get(0));
        copy.set(firstIdx, merged);
        for (int i = 1; i < segments.size(); i++) {
            copy.remove(firstIdx + 1);
        }
        this.segments = copy;

    }

    public Log<T> current() {
        return current;
    }

    public LogIterator<Log<T>> segments(Direction direction) {
        ArrayList<Log<T>> copy = new ArrayList<>(segments);
        return Direction.FORWARD.equals(direction) ? Iterators.of(copy) : Iterators.reversed(copy);
    }

    @Override
    public String toString() {
        return "Levels{" + "depth=" + depth() + ", items=" + Arrays.toString(segments.toArray()) + '}';
    }
}