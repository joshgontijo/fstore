package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.ilog.compaction.Compactor;
import io.joshworks.ilog.compaction.combiner.ConcatenateCombiner;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Log<T extends IndexedSegment> {

    protected final View<T> view;
    private final FlushMode flushMode;
    private final Compactor<T> compactor;

    private final List<LogIterator> forwardIterators = new ArrayList<>();

    public Log(File root,
               int indexSize,
               int compactionThreshold,
               int compactionThreads,
               FlushMode flushMode,
               RowKey rowKey,
               SegmentFactory<T> segmentFactory) throws IOException {
        FileUtils.createDir(root);
        this.flushMode = flushMode;

        if (indexSize > Index.MAX_SIZE) {
            throw new IllegalArgumentException("Index cannot be greater than " + Index.MAX_SIZE);
        }

        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + root.getAbsoluteFile());
        }
        this.view = new View<>(root, rowKey, indexSize, segmentFactory);
        this.compactor = new Compactor<>(view, new ConcatenateCombiner(), compactionThreshold, compactionThreads);
    }

    public void append(Records records) {
        try {
            int offset = 0;
            while (offset < records.size()) {
                IndexedSegment head = getHeadOrRoll();
                int items = head.append(records, offset);
                offset += items;
            }
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to append entry", e);
        }

        if (FlushMode.ALWAYS.equals(flushMode)) {
            getHeadOrRoll().flush();
        }
    }

    private IndexedSegment getHeadOrRoll() {
        IndexedSegment head = view.head();
        if (head.isFull()) {
            head = roll(head);
        }
        return head;
    }

    private IndexedSegment roll(IndexedSegment head) {
        if (FlushMode.ON_ROLL.equals(flushMode) || FlushMode.ALWAYS.equals(flushMode)) {
            head.flush();
        }
        for (LogIterator iterator : forwardIterators) {
            iterator.add(head.iterator(IndexedSegment.START, pool));
        }
        head = rollInternal();
        return head;
    }

    public <R> R apply(Direction direction, Function<List<T>, R> func) {
        return view.apply(direction, func);
    }

    public LogIterator iterator() {
        return view.apply(Direction.FORWARD, segs -> {
            List<SegmentIterator> iterators = segs.stream()
                    .map(seg -> seg.iterator(IndexedSegment.START, pool))
                    .collect(Collectors.toCollection(CopyOnWriteArrayList::new));

            return registerIterator(iterators);
        });
    }

    public LogIterator registerIterator(List<SegmentIterator> iterators) {
        LogIterator logIterator = new LogIterator(iterators);
        this.forwardIterators.add(logIterator);
        return logIterator;
    }

    public void roll() {
        rollInternal();
    }

    private T rollInternal() {
        try {
            T newHead = view.roll();
            compactor.compact(false);
            return newHead;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public long entries() {
        return view.entries();
    }

    public void flush() {
        IndexedSegment head = view.head();
        head.flush();
    }

    public void close() {
        try {
            flush();
            view.close();
        } catch (Exception e) {
            throw new RuntimeIOException("Error while closing segment", e);
        }
    }

    public void delete() {
        view.delete();
    }
}
