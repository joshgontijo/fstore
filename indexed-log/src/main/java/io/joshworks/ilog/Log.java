package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;

public class Log {

    private final View view;
    private final int maxEntrySize;
    private final FlushMode flushMode;

    public Log(File root,
               int maxEntrySize,
               int indexSize,
               FlushMode flushMode,
               BiFunction<File, Index, IndexedSegment> segmentFactory,
               BiFunction<File, Integer, Index> indexFactory) throws IOException {

        this.maxEntrySize = maxEntrySize;
        this.flushMode = flushMode;
        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + root.getAbsoluteFile());
        }
        this.view = new View(root, indexSize, segmentFactory, indexFactory);
    }

    public void append(Record record) {
        try {
            int recordLength = record.recordLength();
            if (recordLength > maxEntrySize) {
                throw new IllegalArgumentException("Record to large, max allowed size: " + maxEntrySize + ", record size: " + recordLength);
            }
            IndexedSegment head = view.head();
            if (head.isFull()) {
                if (FlushMode.ON_ROLL.equals(flushMode)) {
                    head.flush();
                }
                head = view.roll();
            }
            head.append(record);
            if (FlushMode.ALWAYS.equals(flushMode)) {
                head.flush();
            }
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to append entry", e);
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
}
