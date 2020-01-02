package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class Log {

    public static final String EXT = ".log";
    private final View view;
    private final File root;
    private final ByteBuffer writeBuffer;
    private final int maxIndexSize;
    private final AtomicLong offset = new AtomicLong();

    public Log(File root, int maxEntrySize, int maxIndexSize) throws IOException {
        this.root = root;
        this.writeBuffer = Buffers.allocate(maxEntrySize, false);
        this.maxIndexSize = maxIndexSize;
        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + root.getAbsoluteFile());
        }
        this.view = new View(root);
    }

    public long append(ByteBuffer data) {
        writeBuffer.clear();
        IndexedSegment head = view.head();
        if (head.isFull()) {

        }
        long recordOffset = offset.getAndIncrement();
        Record record = Record.create(data, recordOffset);
        head.append(record, writeBuffer);
        return record.offset;
    }

    private class View {
        private final ConcurrentSkipListMap<Long, IndexedSegment> segments = new ConcurrentSkipListMap<>();

        private View(File root) throws IOException {
            for (File file : root.listFiles()) {
                String name = file.getName();
                if (name.endsWith(EXT)) {
                    String n = name.split("\\.")[0];
                    long startOffset = Long.parseLong(n);
                    segments.put(startOffset, new IndexedSegment(file, maxIndexSize, true));
                }
            }
            if (segments.isEmpty()) {
                segments.put(0L, create(0));
            }
        }

        private IndexedSegment get(long offset) {
            Map.Entry<Long, IndexedSegment> entry = segments.floorEntry(offset);
            return entry == null ? null : entry.getValue();
        }

        private void add(long initialOffset, IndexedSegment segment) {
            this.segments.put(initialOffset, segment);
        }

        private IndexedSegment head() {
            return Optional.ofNullable(this.segments.lastEntry()).map(Map.Entry::getValue).orElse(null);
        }

        private boolean isEmpty() {
            return segments.isEmpty();
        }

        private IndexedSegment create(long offset) {
            try {
                int digits = (int) (Math.log10(Long.MAX_VALUE) + 1);
                String name = format("%0" + digits + "d", offset) + EXT;
                return new IndexedSegment(new File(root, name), maxIndexSize, false);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create segment file");
            }
        }

        private void roll() {
            head().roll();
            long off = offset.get();
            segments.put(off, create(off));
        }

    }
}
