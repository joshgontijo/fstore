package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.compaction.Compactor;
import io.joshworks.ilog.compaction.combiner.ConcatenateCombiner;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class Log<T extends IndexedSegment> {

    protected final View<T> view;
    private final FlushMode flushMode;
    private final Compactor<T> compactor;

    public static final long START = 0;

    public Log(File root,
               long levelZeroIndexEntries,
               int compactionThreshold,
               int compactionThreads,
               RowKey rowKey,
               FlushMode flushMode,
               RecordPool pool,
               SegmentFactory<T> segmentFactory) throws IOException {
        FileUtils.createDir(root);
        this.flushMode = flushMode;

        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + root.getAbsoluteFile());
        }
        this.view = new View<>(root, pool, rowKey, levelZeroIndexEntries, segmentFactory);
        this.compactor = new Compactor<>(view, new ConcatenateCombiner(), compactionThreshold, compactionThreads);
    }

    public void append(Records records) {
        try {
            int size = records.size();
            int inserted = 0;
            while (inserted < size) {
                IndexedSegment head = getHeadOrRoll();
                inserted += head.append(records, inserted);
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
        if (head.index().isFull()) {
            head = roll(head);
        }
        return head;
    }

    private IndexedSegment roll(IndexedSegment head) {
        if (FlushMode.ON_ROLL.equals(flushMode) || FlushMode.ALWAYS.equals(flushMode)) {
            head.flush();
        }
        head = rollInternal();
        return head;
    }

    public <R> R apply(Direction direction, Function<List<T>, R> func) {
        return view.apply(direction, func);
    }


    public void roll() {
        rollInternal();
    }

    private T rollInternal() {
        T newHead = view.roll();
        compactor.compact(false);
        return newHead;
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
