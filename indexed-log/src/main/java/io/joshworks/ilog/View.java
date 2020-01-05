package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class View {

    private static final Logger log = LoggerFactory.getLogger(View.class);

    public static final String EXT = ".log";
    private final ConcurrentSkipListMap<Long, IndexedSegment> segments = new ConcurrentSkipListMap<>();

    private final AtomicLong entries = new AtomicLong();
    private final File root;
    private final int indexSize;
    private final BiFunction<File, Index, IndexedSegment> segmentFactory;
    private final BiFunction<File, Integer, Index> indexFactory;

    View(File root, int indexSize, BiFunction<File, Index, IndexedSegment> segmentFactory, BiFunction<File, Integer, Index> indexFactory) throws IOException {
        this.root = root;
        this.indexSize = indexSize;
        this.segmentFactory = segmentFactory;
        this.indexFactory = indexFactory;

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

    IndexedSegment head() {
        return Optional.ofNullable(this.segments.lastEntry()).map(Map.Entry::getValue).orElse(null);
    }

    public long entries() {
        return entries.get() + head().entries();
    }

    private void reopen(File f, Function<File, IndexedSegment> fun) {
        long startOffset = parseLogName(f);
        IndexedSegment segment = fun.apply(f);
        segments.put(startOffset, segment);
        entries.addAndGet(segment.entries());
    }

    private IndexedSegment get(long offset) {
        Map.Entry<Long, IndexedSegment> entry = segments.floorEntry(offset);
        return entry == null ? null : entry.getValue();
    }

    private IndexedSegment open(File segmentFile) {
        try {
            File indexFile = indexFile(segmentFile);
            Index index = indexFactory.apply(indexFile, indexSize);
            return segmentFactory.apply(segmentFile, index);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment " + segmentFile.getName(), e);
        }
    }

    private IndexedSegment openHead(File segmentFile) {
        try {
            IndexedSegment segment = open(segmentFile);
            File indexFile = indexFile(segmentFile);
            FileUtils.deleteIfExists(indexFile);
            Index index = indexFactory.apply(indexFile, indexSize);
            segment.reindex(index);
            segment.roll();
            return segment;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment " + segmentFile.getName(), e);
        }
    }

    private IndexedSegment create(long offset) {
        try {
            File segmentFile = segmentFile(offset);
            File indexFile = indexFile(segmentFile);
            Index index = indexFactory.apply(indexFile, indexSize);
            return segmentFactory.apply(segmentFile, index);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create segment file");
        }
    }

    private File segmentFile(long offset) {
        int digits = (int) (Math.log10(Long.MAX_VALUE) + 1);
        String name = format("%0" + digits + "d", offset) + EXT;
        return new File(root, name);
    }

    private File indexFile(File segmentFile) {
        String name = segmentFile.getName().split("\\.")[0];
        File dir = segmentFile.toPath().getParent().toFile();
        return new File(dir, name + ".index");
    }

    IndexedSegment roll() throws IOException {
        long start = System.currentTimeMillis();
        IndexedSegment head = head();
        log.info("Rolling segment {}", head);
        head.roll();
        long entryCount = entries.addAndGet(head.entries());
        IndexedSegment newHead = create(entryCount);
        segments.put(entryCount, newHead);
        log.info("Segment {} rolled in {}ms", head, System.currentTimeMillis() - start);
        return newHead;
    }

    private static long parseLogName(File file) {
        String name = file.getName().split("\\.")[0];
        return Long.parseLong(name);
    }

    public void close() throws IOException {
        for (IndexedSegment segment : segments.values()) {
            log.info("Closing segment {}", segment);
            segment.close();
        }
    }
}