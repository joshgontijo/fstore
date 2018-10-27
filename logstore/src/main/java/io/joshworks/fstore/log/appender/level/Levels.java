package io.joshworks.fstore.log.appender.level;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.header.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

//LEVEL0 [CURRENT_SEGMENT]
//LEVEL1 [SEG1][SEG2]
//LEVEL2 [SEG3][SEG4]
//LEVEL3 ...
public class Levels<T> {

    private volatile List<Log<T>> segments = new CopyOnWriteArrayList<>();
    private volatile Log<T> current;

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

    public List<Log<T>> segments(int level) {
        return segments.stream().filter(seg -> seg.level() == level).collect(Collectors.toList());
    }

    public Log<T> get(int segmentIdx) {
        int size = segments.size();
        if (segmentIdx < 0 || segmentIdx >= size) {
            throw new IllegalArgumentException("Invalid segment idx: " + segmentIdx + ", size: " + size);
        }
        return segments.get(segmentIdx);
    }

    public int depth() {
        return segments.stream().mapToInt(Log::level).max().orElse(0);
    }

    public static synchronized <T> Levels<T> create(List<Log<T>> segments) {
        return new Levels<>(segments);
    }

    public synchronized void appendSegment(Log<T> newHead) {
        if (newHead.level() != 0) {
            throw new IllegalArgumentException("New segment must be level zero");
        }
        int size = segments.size();
        if (size == 0) {
            segments.add(newHead);
            return;
        }
        Log<T> currentSegment = current();

//        prevHead.roll(1);
        if (!currentSegment.readOnly()) {
            throw new IllegalStateException("Segment must be marked as read only after rolling");
        }
        segments.add(newHead);
        current = newHead;
    }

    public int numSegments() {
        return segments.size();
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

        Log<T> first = segments.get(0);
        int level = first.level(); //safe to assume that there is a segment and all of them are the same level
        int nextLevel = level + 1;
        int firstIdx = copy.indexOf(first);
        copy.set(firstIdx, merged);
        for (int i = 1; i < segments.size(); i++) {
            copy.remove(firstIdx + 1);
        }
        //TODO if the program cash after here, then there could be multiple READ_ONLY segments with same data
        //ideally segments would have the source segments in their header to flag where they're were created from.
        //on startup read all the source segments from the header and if there's an existing segment, then delete, either the sources
        //or the target segment
        merged.roll(nextLevel);
        this.segments = copy;

    }

    public Log<T> current() {
        return current;
    }

    public synchronized LogIterator<Log<T>> segments(Direction direction) {
        ArrayList<Log<T>> copy = new ArrayList<>(segments);
        return Direction.FORWARD.equals(direction) ? Iterators.of(copy) : Iterators.reversed(copy);
    }

    @Override
    public String toString() {
        return "Levels{" + "depth=" + depth() + ", items=" + Arrays.toString(segments.toArray()) + '}';
    }
}