package io.joshworks.es;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.BitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;

/**
 * Not Thread safe, synchronization must be done outside
 * Mainly because EventStore needs to sync on both Log and Index a the same time, adding here would just be unecessary overhead
 * head() does not need sync as it does not change
 */
public class SegmentDirectory<T extends SegmentFile> {

    private static final Logger log = LoggerFactory.getLogger(SegmentDirectory.class);
    private static final String SEPARATOR = "-";

    private final Map<Long, T> segments = new ConcurrentHashMap<>();
    private final AtomicLong headIdx = new AtomicLong(-1);

    private final File root;
    private final String extension;
    private final long maxFiles;

    protected SegmentDirectory(File root, String extension, long maxFiles) {
        this.root = root;
        this.extension = extension;
        this.maxFiles = maxFiles;

        initDirectory(root);
    }

    private static void initDirectory(File root) {
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
            Map<Long, Optional<T>> items = Files.list(root.toPath())
                    .map(Path::toFile)
                    .filter(this::matchExtension)
                    .map(fn)
                    .collect(groupingBy(SegmentDirectory::segmentIdx,
                            reducing(SegmentDirectory::removeHigherLevel)));

            items.values().stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(this::add);

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to load segments", e);
        }
    }

    private long computeHeadIx() {
        return segments.keySet().stream().mapToLong(k -> k).max().orElse(-1);
    }

    private static <T extends SegmentFile> T removeHigherLevel(T t1, T t2) {
        if (SegmentDirectory.level(t1) > SegmentDirectory.level(t2)) {
            delete(t2);
            return t1;
        }
        delete(t1);
        return t2;
    }

    private static <T extends SegmentFile> void delete(T sf) {
        log.info("REMOVING [" + sf + "]");
        sf.delete();
    }

    //add to the head
    public void add(T segment) {
        long idx = SegmentDirectory.segmentIdx(segment);
        long expected = headIdx() + 1;
        if (expected != idx) {
            throw new IllegalStateException("Expected segmentIdx: " + expected + " got " + idx);
        }
        segments.put(idx, segment);
        headIdx.set(computeHeadIx());
    }

    public void merge(T replacement, Collection<T> items) {
        Set<T> files = new HashSet<>(items);

        validateMergeFiles(files);
        int expectedNextLevel = computeNextLevel(files);
        long expectedSegmentIdx = validateSequentialFiles(files);

        int replacementLevel = level(replacement);
        long replacementIdx = segmentIdx(replacement);
        if (replacementLevel != expectedNextLevel) {
            throw new IllegalArgumentException("Expected level " + expectedNextLevel + ", got " + replacementLevel);
        }
        if (replacementIdx != expectedSegmentIdx) {
            throw new IllegalArgumentException("Expected segmentIdx " + expectedSegmentIdx + ", got " + replacementIdx);
        }

        for (T file : files) {
            long idx = segmentIdx(file);
            this.segments.remove(idx);
            file.delete();
        }
        this.segments.put(replacementIdx, replacement);
    }

    public int computeNextLevel(Set<T> files) {
        return files.stream().mapToInt(SegmentDirectory::level).max().getAsInt() + 1;
    }

    public int segments() {
        return segments.size();
    }

    public List<String> segmentsNames() {
        return segments.values().stream().map(s -> s.file().getName()).collect(Collectors.toList());
    }

    public File root() {
        return root;
    }

    protected long headIdx() {
        return headIdx.get();
    }

    protected T head() {
        return segments.get(headIdx());
    }

    public int depth() {
        return segments.values().stream().mapToInt(SegmentDirectory::level).max().orElse(0);
    }

    public File newMergeFile(Collection<T> items) {
        Set<T> segments = new HashSet<>(items);
        validateMergeFiles(segments);
        int nextLevel = computeNextLevel(segments);

        long baseSegmentIdx = validateSequentialFiles(segments);
        return newSegmentFile(baseSegmentIdx, nextLevel);
    }

    private void validateMergeFiles(Set<T> files) {
        if (files.contains(head())) {
            throw new IllegalArgumentException("Cannot merge head file");
        }
        if (!new HashSet<>(segments.values()).containsAll(files)) {
            throw new IllegalArgumentException("Invalid segment files");
        }
    }

    private long validateSequentialFiles(Set<T> files) {
        long startIdx = files.stream().mapToLong(SegmentDirectory::segmentIdx).min().getAsLong();
        long endIdx = files.stream().mapToLong(SegmentDirectory::segmentIdx).max().getAsLong();

        for (long i = startIdx; i <= endIdx; i++) {
            T seg = segments.get(i);
            if (seg != null && !files.contains(seg)) {
                throw new IllegalArgumentException("Non sequential segment files");
            }
        }

        return startIdx;
    }

    protected File newHeadFile() {
        long nextSegIdx = headIdx() + 1;
        if (nextSegIdx > maxFiles) {
            throw new RuntimeException("Segment files limit reached " + maxFiles);
        }
        File segmentFile = newSegmentFile(nextSegIdx, 0);
        ensureNoDuplicates(segmentFile);
        return segmentFile;
    }

    private void ensureNoDuplicates(File segmentFile) {
        for (var segment : segments.values()) {
            if (name(segment.file()).equals(name(segmentFile))) {
                throw new IllegalStateException("Duplicate segment name");
            }
        }
    }

    public void close() {
        for (var entry : segments.values()) {
            entry.close();
        }
        segments.clear();
        headIdx.set(-1);
    }

    private File newSegmentFile(long segmentIdx, int level) {
        String name = segmentFileName(segmentIdx, level, extension, maxFiles);
        return new File(root, name);
    }

    public static String segmentFileName(long segmentIdx, int level, String ext, long maxFiles) {
        if (segmentIdx < 0 || level < 0) {
            throw new RuntimeException("Invalid segment values, level: " + level + ", idx: " + segmentIdx);
        }
        return String.format("%02d", level) + "-" + String.format("%0" + BitUtil.decimalUnitsForDecimal(maxFiles) + "d", segmentIdx) + "." + ext;
    }

    private boolean matchExtension(File file) {
        return file.getName().endsWith(extension);
    }

    //------------------------

    public static long segmentIdx(SegmentFile sf) {
        return Long.parseLong(name(sf.file()).split(SEPARATOR)[1]);
    }

    private static String name(File file) {
        return file.getName().split("\\.")[0];
    }

    public static int level(SegmentFile sf) {
        return Integer.parseInt(name(sf.file()).split(SEPARATOR)[0]);
    }

    protected T tryGet(long segmentIdx) {
        return segments.get(segmentIdx);
    }

    //throw exception if segment does not exist
    protected T getSegment(long segmentIdx) {
        T seg = tryGet(segmentIdx);
        if (seg == null) {
            throw new IllegalStateException("No segment for index " + segmentIdx);
        }
        return seg;
    }

    protected int size() {
        return segments.size();
    }

    protected boolean isEmpty() {
        return segments.isEmpty();
    }

}
