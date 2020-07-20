package io.joshworks.es;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.BitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;


public class SegmentDirectory<T extends SegmentFile> {

    private static final Logger log = LoggerFactory.getLogger(SegmentDirectory.class);
    private static final String LEVEL_SEPARATOR = "-";

    //    protected final SortedSet<T> segments = new TreeSet<>(SegmentDirectory::compare);
    protected final List<T> segments = new CopyOnWriteArrayList<>();

    private volatile T head;

    private final File root;
    private final String extension;

    protected SegmentDirectory(File root, String extension) {
        this.root = root;
        this.extension = extension;

        try {
            if (!Files.exists(root.toPath())) {
                Files.createDirectory(root.toPath());
            }
            if (!root.isDirectory()) {
                throw new IllegalArgumentException("Not a directory " + root.getAbsolutePath());
            }

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize segment directory", e);
        }
    }

    protected void loadSegments(Function<File, T> fn) {
        try {
            Files.list(root.toPath())
                    .map(Path::toFile)
                    .filter(this::matchExtension)
                    .map(fn)
                    .sorted(SegmentDirectory::compare)
                    .forEach(entry -> {
                        System.out.println("Loaded " + entry.file());
                        segments.add(entry);
                    });
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to load segments", e);
        }
    }

    protected void makeHead(T item) {
        this.head = item;
    }

    protected T head() {
        if (head == null) {
            throw new IllegalStateException("No head segment found");
        }
        return head;
    }

    protected File newSegmentFile(int level) {
        int nextSegIdx = nextSegmentIdx();
        File segmentFile = newSegmentFile(nextSegIdx, level);
        for (T segment : segments) {
            if (name(segment.file()).equals(name(segmentFile))) {
                throw new IllegalStateException("Duplicate segment name");
            }
        }
        return segmentFile;
    }


    public void close() {
        for (T segment : segments) {
            log.info("Closing segment {}", segment);
            segment.close();
        }
        segments.clear();
    }

    private int nextSegmentIdx() {
        return segments
                .stream()
                .mapToInt(SegmentDirectory::segmentIdx)
                .max()
                .orElse(-1) + 1;
    }


    private File newSegmentFile(int segmentIdx, int level) {
        String name = segmentFileName(segmentIdx, level);
        return new File(root, name);
    }

    private String segmentFileName(int segmentIdx, int level) {
        if (segmentIdx < 0 || level < 0) {
            throw new RuntimeException("Invalid segment values, level: " + level + ", idx: " + segmentIdx);
        }
        return String.format("%02d", level) + "-" + String.format("%0" + BitUtil.decimalUnitsForBits(Long.MAX_VALUE) + "d", segmentIdx) + "." + extension;
    }

    private boolean matchExtension(File file) {
        return file.getName().endsWith(extension);
    }

    //------------------------

    private static int compare(SegmentFile o1, SegmentFile o2) {
        int levelDiff = level(o2) - level(o1);
        if (levelDiff == 0) {
            int createdDiff = Long.compare(segmentIdx(o1), segmentIdx(o2));
            if (createdDiff != 0)
                return createdDiff;
        }
        return levelDiff;
    }

    public static int segmentIdx(SegmentFile sf) {
        return Integer.parseInt(name(sf.file()).split(LEVEL_SEPARATOR)[1]);
    }

    private static String name(File file) {
        return file.getName().split("\\.")[0];
    }

    private static int level(SegmentFile sf) {
        return Integer.parseInt(name(sf.file()).split(LEVEL_SEPARATOR)[0]);
    }

}
