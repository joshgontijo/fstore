package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.ilog.compaction.Compactor;
import io.joshworks.ilog.compaction.combiner.ConcatenateCombiner;
import io.joshworks.ilog.index.Index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

public class Log<T extends IndexedSegment> {

    protected final View<T> view;
    private final File root;
    private final int maxEntrySize;
    private final FlushMode flushMode;
    protected final BufferPool pool;
    private final Compactor<T> compactor;

    public Log(File root,
               int maxEntrySize,
               int indexSize,
               int compactionThreshold,
               FlushMode flushMode,
               BufferPool pool,
               SegmentFactory<T> segmentFactory) throws IOException {
        this.root = root;
        this.maxEntrySize = maxEntrySize;
        this.flushMode = flushMode;
        this.pool = pool;

        if (indexSize > Index.MAX_SIZE) {
            throw new IllegalArgumentException("Index cannot be greater than " + Index.MAX_SIZE);
        }

        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + root.getAbsoluteFile());
        }
        var reindexPool = BufferPool.unpooled(Math.max(maxEntrySize, Memory.PAGE_SIZE), false);
        this.view = new View<>(root, indexSize, reindexPool, segmentFactory);
        this.compactor = new Compactor<>(view, "someName", new ConcatenateCombiner(pool), true, compactionThreshold);
    }

    public void append(ByteBuffer record) {
        try {
            int recordLength = Record2.validate(record);
            if (recordLength > maxEntrySize) {
                throw new IllegalArgumentException("Record to large, max allowed size: " + maxEntrySize + ", record size: " + recordLength);
            }
            IndexedSegment head = view.head();
            if (head.isFull()) {
                if (FlushMode.ON_ROLL.equals(flushMode)) {
                    head.flush();
                }
                head = view.roll();
                compactor.compact(false);
            }
            head.append(record);
            if (FlushMode.ALWAYS.equals(flushMode)) {
                head.flush();
            }
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to append entry", e);
        }
    }

    public <R> R apply(Function<List<T>, R> func) {
        return view.apply(Direction.FORWARD, func);
    }

    public void roll() {
        try {
            view.roll();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public long entries() {
        return view.entries();
    }

    public void flush() {
        IndexedSegment head = view.head();
        try {
            head.flush();
        } catch (IOException e) {
            throw new RuntimeIOException("Failed flushing " + head, e);
        }
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
