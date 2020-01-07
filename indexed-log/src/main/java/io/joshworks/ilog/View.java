package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
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
import java.util.stream.Collectors;

import static java.lang.String.format;

public class View {

    private static final Logger log = LoggerFactory.getLogger(View.class);
    private static final String EXT = ".log";

    private final List<IndexedSegment> segments = new CopyOnWriteArrayList<>();

    private final AtomicLong nextSegmentIdx = new AtomicLong();
    private final AtomicLong entries = new AtomicLong();
    private final File root;
    private final int indexSize;
    private final BiFunction<File, Index, IndexedSegment> segmentFactory;
    private final BiFunction<File, Integer, Index> indexFactory;
    public static final int LEVEL_DIGITS = 3;
    public static final int SEG_IDX_DIGITS = (int) (Math.log10(Long.MAX_VALUE) + 1);

    View(File root, int indexSize, BiFunction<File, Index, IndexedSegment> segmentFactory, BiFunction<File, Integer, Index> indexFactory) throws IOException {
        this.root = root;
        this.indexSize = indexSize;
        this.segmentFactory = segmentFactory;
        this.indexFactory = indexFactory;

        List<IndexedSegment> segments = Files.list(root.toPath())
                .map(Path::toFile)
                .filter(f -> f.getName().endsWith(EXT))
                .map(this::open)
                .sorted(compareSegments())
                .collect(Collectors.toList());

        entries.set(segments.stream().mapToLong(IndexedSegment::entries).sum());

        if (!segments.isEmpty()) {
            IndexedSegment head = segments.get(segments.size() - 1);
            head.reindex(indexFactory);
            head.forceRoll();

            nextSegmentIdx.set(head.segmentId() + 1);
        }
        this.segments.add(createHead());
    }


    private static Comparator<IndexedSegment> compareSegments() {
        return (o1, o2) -> {
            int levelDiff = o2.level() - o1.level();
            if (levelDiff == 0) {
                int createdDiff = Long.compare(o1.segmentId(), o2.segmentId());
                if (createdDiff != 0)
                    return createdDiff;
            }
            return levelDiff;
        };
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

    private IndexedSegment open(File segmentFile) {
        try {
            File indexFile = indexFile(segmentFile);
            Index index = indexFactory.apply(indexFile, indexSize);
            return segmentFactory.apply(segmentFile, index);
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
        String name = format("%0" + SEG_IDX_DIGITS + "d", segmentIdx) + "-" + format("%0" + LEVEL_DIGITS + "d", level) + EXT;
        return new File(root, name);
    }

    public static long segmentIdx(String name) {
        return Long.parseLong(name.split("-")[0]);
    }

    public static int levelOf(String name) {
        return Integer.parseInt(name.split("-")[1]);
    }

    static File indexFile(File segmentFile) {
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

    public void close() throws IOException {
        for (IndexedSegment segment : segments) {
            log.info("Closing segment {}", segment);
            segment.close();
        }
    }
}