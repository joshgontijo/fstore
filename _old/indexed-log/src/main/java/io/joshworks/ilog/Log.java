package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.compaction.CompactionRunner;
import io.joshworks.ilog.compaction.combiner.SegmentCombiner;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Log<T extends Segment> implements Iterable<Record> {

    protected final View view;
    private final FlushMode flushMode;
    private final CompactionRunner compactionRunner;
    private final Set<LogIterator> forwardIterators = new HashSet<>();

    public Log(File root,
               long maxLogSize,
               long maxLogEntries,
               int compactionThreshold,
               SegmentCombiner combiner,
               FlushMode flushMode,
               RecordPool pool,
               SegmentFactory<T> segmentFactory) {
        FileUtils.createDir(root);
        this.flushMode = flushMode;

        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + root.getAbsoluteFile());
        }
        this.view = new View(root, pool, maxLogSize, maxLogEntries, segmentFactory);
        this.compactionRunner = new CompactionRunner(compactionThreshold, combiner);
    }

    public void append(Records records) {
        try {
            int size = records.size();
            int inserted = 0;
            while (inserted < size) {
                Segment head = getHeadOrRoll();
                inserted += head.append(records, inserted);
            }
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to append entry", e);
        }

        if (FlushMode.ALWAYS.equals(flushMode)) {
            getHeadOrRoll().flush();
        }
    }

    @Override
    public Iterator<Record> iterator() {
        return view.apply(Direction.FORWARD, segments -> {
            List<SegmentIterator> iterators = segments.stream().map(Segment::iterator).collect(Collectors.toList());
            return createIterator(iterators);
        });
    }

    private Segment getHeadOrRoll() {
        Segment head = view.head();
        if (head.isFull()) {
            head = roll(head);
        }
        return head;
    }

    private Segment roll(Segment head) {
        if (FlushMode.ON_ROLL.equals(flushMode) || FlushMode.ALWAYS.equals(flushMode)) {
            head.flush();
        }
        head = rollInternal();

        //add new segment to iterators
        for (LogIterator it : forwardIterators) {
            it.add(head.iterator());
        }

        return head;
    }

    public <R> R apply(Direction direction, Function<List<T>, R> func) {
        return view.apply(direction, func);
    }


    public void roll() {
        rollInternal();
    }

    private Segment rollInternal() {
        Segment newHead = view.roll();
        compactionRunner.compact(view, false);
        return newHead;
    }

    public long entries() {
        return view.entries();
    }

    public void flush() {
        Segment head = view.head();
        head.flush();
    }

    public void close() {
        try {
            closeIterators();
            flush();
            view.close();

        } catch (Exception e) {
            throw new RuntimeIOException("Error while closing segment", e);
        }
    }

    public void delete() {
        view.delete();
    }

    private Iterator<Record> createIterator(List<SegmentIterator> segIterators) {
        LogIterator iterator = new LogIterator(segIterators, this::removeIterator);
        forwardIterators.add(iterator);
        return iterator;
    }

    void removeIterator(LogIterator logIterator) {
        forwardIterators.remove(logIterator);
    }

    private void closeIterators() {
        for (LogIterator it : forwardIterators) {
            it.close();
        }
        forwardIterators.clear();
    }
}
