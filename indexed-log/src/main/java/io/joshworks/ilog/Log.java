package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.io.IOException;

public class Log<K extends Comparable<K>> {

    private final View<K> view;
    private final int maxEntrySize;

    public Log(File root, int maxEntrySize, int indexSize, KeyParser<K> parser) throws IOException {
        this.maxEntrySize = maxEntrySize;
        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + root.getAbsoluteFile());
        }
        this.view = new View<>(root, indexSize, parser);
    }

    public void append(Record record) {
        try {
            int recordLength = record.recordLength();
            if (recordLength > maxEntrySize) {
                throw new IllegalArgumentException("Record to large, max allowed size: " + maxEntrySize + ", record size: " + recordLength);
            }
            IndexedSegment<K> head = view.head();
            if (head.isFull()) {
                head = view.roll();
            }
            head.append(record);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to append entry", e);
        }
    }

}
