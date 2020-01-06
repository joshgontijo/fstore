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
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class View {

    private static final Logger log = LoggerFactory.getLogger(View.class);
    private static final String EXTENSION_PATTERN = "[0-9.]+$";

    private final List<IndexedSegment> segments = new CopyOnWriteArrayList<>();

    private final AtomicLong nextSegmentIdx = new AtomicLong();
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
                .filter(f -> f.getName().matches(EXTENSION_PATTERN))
                .sorted(Comparator.comparingLong(View::parseSegmentId))
                .collect(Collectors.toList());

        for (int i = 0; i < segmentFiles.size() - 1; i++) {
            File segFile = segmentFiles.get(i);
            reopen(segFile, this::open);
        }

        if (!segmentFiles.isEmpty()) {
            File segFile = segmentFiles.get(segmentFiles.size() - 1);
            reopen(segFile, this::openHead);
            nextSegmentIdx.set(parseSegmentId(segFile) + 1);
        }
        this.segments.add(createHead());
    }

    IndexedSegment head() {
        int size = segments.size();
        if (size == 0) {
            return null;
        }
        return Optional.ofNullable(segments.get(size - 1)).orElse(null);
    }

    public long entries() {
        return entries.get() + head().entries();
    }

    private void reopen(File f, Function<File, IndexedSegment> fun) {
        IndexedSegment segment = fun.apply(f);
        segments.add(segment);
        entries.addAndGet(segment.entries());
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

    private IndexedSegment createHead() {
        try {
            long nextSegIdx = nextSegmentIdx.getAndIncrement();
            File segmentFile = segmentFile(nextSegIdx, 0);
            File indexFile = indexFile(segmentFile);
            Index index = indexFactory.apply(indexFile, indexSize);
            return segmentFactory.apply(segmentFile, index);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create segment file");
        }
    }

    private File segmentFile(long segmentIdx, int level) {
        int digits = (int) (Math.log10(Long.MAX_VALUE) + 1);
        String name = format("%0" + digits + "d", segmentIdx) + "." + format("%03d", level);
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
        entries.addAndGet(head.entries());
        IndexedSegment newHead = createHead();
        segments.add(newHead);
        log.info("Segment {} rolled in {}ms", head, System.currentTimeMillis() - start);
        return newHead;
    }

    public static long parseSegmentId(File file) {
        String name = file.getName().split("\\.")[0];
        return Long.parseLong(name);
    }

    public static int parseLevel(File file) {
        return Integer.parseInt(file.getName().split("\\.")[1]);
    }

    public void close() throws IOException {
        for (IndexedSegment segment : segments) {
            log.info("Closing segment {}", segment);
            segment.close();
        }
    }
}