package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class View<K extends Comparable<K>> {

    public static final String EXT = ".log";
    private final ConcurrentSkipListMap<Long, IndexedSegment<K>> segments = new ConcurrentSkipListMap<>();

    private final AtomicLong entries = new AtomicLong();
    private final File root;
    private final int indexSize;
    private final KeyParser<K> parser;

    View(File root, int indexSize, KeyParser<K> parser) throws IOException {
        this.root = root;
        this.indexSize = indexSize;
        this.parser = parser;

        List<File> segmentFiles = Files.list(root.toPath())
                .map(Path::toFile)
                .filter(f -> f.getName().endsWith(EXT))
                .sorted(Comparator.comparingLong(View::parseLogName))
                .collect(Collectors.toList());

        for (int i = 0; i < segmentFiles.size() - 1; i++) {
            File segFile = segmentFiles.get(i);
            reopen(segFile, this::open);
        }

        if (!segmentFiles.isEmpty()) {
            File segFile = segmentFiles.get(segmentFiles.size() - 1);
            reopen(segFile, this::openHead);
        }
        long currOffset = entries.get();
        this.segments.put(currOffset, create(currOffset));
    }

    IndexedSegment<K> head() {
        return Optional.ofNullable(this.segments.lastEntry()).map(Map.Entry::getValue).orElse(null);
    }

    public long entries() {
        return entries.get() + head().entries();
    }

    private void reopen(File f, Function<File, IndexedSegment<K>> fun) {
        long startOffset = parseLogName(f);
        IndexedSegment<K> segment = fun.apply(f);
        segments.put(startOffset, segment);
        entries.addAndGet(segment.entries());
    }

    private IndexedSegment<K> get(long offset) {
        Map.Entry<Long, IndexedSegment<K>> entry = segments.floorEntry(offset);
        return entry == null ? null : entry.getValue();
    }

    private IndexedSegment<K> open(File file) {
        try {
            return new IndexedSegment<>(file, indexSize, parser);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment " + file.getName(), e);
        }
    }

    private IndexedSegment<K> openHead(File file) {
        try {
            IndexedSegment<K> segment = open(file);
            segment.reindex();
            segment.roll();
            return segment;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment " + file.getName(), e);
        }
    }

    private IndexedSegment<K> create(long offset) {
        try {
            File segmentFile = segmentFile(offset);
            return new IndexedSegment<>(segmentFile, indexSize, parser);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create segment file");
        }
    }

    private File segmentFile(long offset) {
        int digits = (int) (Math.log10(Long.MAX_VALUE) + 1);
        String name = format("%0" + digits + "d", offset) + EXT;
        return new File(root, name);
    }


    IndexedSegment<K> roll() throws IOException {
        head().roll();
        long entryCount = entries();
        IndexedSegment<K> newHead = create(entryCount);
        segments.put(entryCount, newHead);
        return newHead;
    }

    private static long parseLogName(File file) {
        String name = file.getName().split("\\.")[0];
        return Long.parseLong(name);
    }

}