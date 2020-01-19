package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.ilog.compaction.Compactor;
import io.joshworks.ilog.compaction.combiner.ConcatenateCombiner;
import io.joshworks.ilog.index.Index;

import java.io.File;
import java.io.IOException;
import java.util.function.BiFunction;

public class Log {

    private final View view;
    private final int maxEntrySize;
    private final FlushMode flushMode;
    private final Compactor compactor;

    public Log(File root,
               int maxEntrySize,
               int indexSize,
               int compactionThreshold,
               FlushMode flushMode,
               BufferPool pool,
               BiFunction<File, Index, IndexedSegment> segmentFactory,
               BiFunction<File, Integer, Index> indexFactory) throws IOException {

        this.maxEntrySize = maxEntrySize;
        this.flushMode = flushMode;

        if (indexSize > Index.MAX_SIZE) {
            throw new IllegalArgumentException("Index cannot be greater than " + Index.MAX_SIZE);
        }

        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + root.getAbsoluteFile());
        }
        var reindexPool = BufferPool.unpooled(Math.max(maxEntrySize, Memory.PAGE_SIZE), false);
        this.view = new View(root, indexSize, reindexPool, segmentFactory, indexFactory);
        this.compactor = new Compactor(view, "someName", new ConcatenateCombiner(pool), true, compactionThreshold);
    }

    public void append(Record record) {
        try {
            int recordLength = record.size();
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

//    /**
//     * Reads a single entry for the given offset, read is performed with a single IO call
//     * with a buffer of size specified by readSize. If the buffer is too small for the entry, then a new one is created and
//     */
//    public int read(ByteBuffer key, ByteBuffer dst) {
//        int readSize = dst.remaining();
//        if (readSize <= HEADER_BYTES) {
//            throw new RuntimeException("bufferSize must be greater than " + HEADER_BYTES);
//        }
//        return channel.read(dst, position);
//    }

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
