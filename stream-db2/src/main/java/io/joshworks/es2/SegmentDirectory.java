package io.joshworks.es2;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.BitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;

/**
 * Not Thread safe, synchronization must be done outside
 * Mainly because EventStore needs to sync on both Log and Index a the same time, adding here would just be unecessary overhead
 * head() does not need sync as it does not change
 */
public class SegmentDirectory<T extends SegmentFile> {

    private static final Logger log = LoggerFactory.getLogger(SegmentDirectory.class);
    private static final String SEPARATOR = "-";

    private final ConcurrentSkipListSet<T> segments = new ConcurrentSkipListSet<>();

    private final File root;
    private final String extension;

    protected SegmentDirectory(File root, String extension) {
        this.root = root;
        this.extension = extension;

        initDirectory(root);
    }

    public SegmentFile addHead(Function<File, T> create) {
        long segIdx = 0;
        if (!segments.isEmpty()) {
            T currentHead = segments.first();
            int headLevel = level(currentHead);
            if (headLevel == 0) {
                segIdx = segmentIdx(currentHead) + 1;
            }
        }
        File file = newSegmentFile(segIdx, 0);
        T segment = create.apply(file);
        segments.add(segment);
        assert segments.first().equals(segment);
        return segment;
    }

    private static void initDirectory(File root) {
        try {
            if (!root.isDirectory()) {
                throw new IllegalArgumentException("Not a directory " + root.getAbsolutePath());
            }
            Files.createDirectories(root.toPath());
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize segment directory", e);
        }
    }

    public void loadSegments(Function<File, T> fn) {
        try {
            Files.list(root.toPath())
                    .map(Path::toFile)
                    .filter(this::matchExtension)
                    .map(fn)
                    .forEach(segments::add);

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to load segments", e);
        }
    }

    private synchronized void merge(MergeFile<T> merge) {
        Set<T> files = new HashSet<>(merge.sources);
        T replacement = merge.replacement;

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
            this.segments.remove(file);
            file.delete();
        }
        this.segments.add(replacement);
    }

    public int computeNextLevel(Set<T> files) {
        return files.stream().mapToInt(SegmentDirectory::level).max().getAsInt() + 1;
    }

    public int segments() {
        return segments.size();
    }

    public File root() {
        return root;
    }

    protected T head() {
        return segments.first();
    }

//    public MergeFile<T> createMerge(int level) {
//        Set<T> segments = new HashSet<>(items);
//        validateMergeFiles(segments);
//        int nextLevel = computeNextLevel(segments);
//
//        long baseSegmentIdx = validateSequentialFiles(segments);
//        return newSegmentFile(baseSegmentIdx, nextLevel);
//    }

    private static <T extends SegmentFile> int compare(T s1, T s2) {
        return s1.file().getName().compareTo(s2.file().getName());
    }

    private void validateMergeFiles(Set<T> files) {
        if (files.contains(head())) {
            throw new IllegalArgumentException("Cannot merge head file");
        }
        if (!segments.containsAll(files)) {
            throw new IllegalArgumentException("Invalid segment files");
        }
    }

    private long validateSequentialFiles(Set<T> files) {
        long startIdx = files.stream().mapToLong(SegmentDirectory::segmentIdx).min().getAsLong();
        long endIdx = files.stream().mapToLong(SegmentDirectory::segmentIdx).max().getAsLong();

//        for (long i = startIdx; i <= endIdx; i++) {
//            T seg = segments.get(i);
//            if (seg != null && !files.contains(seg)) {
//                throw new IllegalArgumentException("Non sequential segment files");
//            }
//        }

        return startIdx;
    }

//    private SortedSet<T> fromLevel(int level) {
//
////        segments.subSet()
//    }

//    protected File newHeadFile() {
//        long nextSegIdx = headIdx() + 1;
//        if (nextSegIdx > maxFiles) {
//            throw new RuntimeException("Segment files limit reached " + maxFiles);
//        }
//        File segmentFile = newSegmentFile(nextSegIdx, 0);
//        ensureNoDuplicates(segmentFile);
//        return segmentFile;
//    }

    private void ensureNoDuplicates(File segmentFile) {
        for (var segment : segments) {
            if (name(segment.file()).equals(name(segmentFile))) {
                throw new IllegalStateException("Duplicate segment name");
            }
        }
    }

    public void close() {
        for (var entry : segments) {
            entry.close();
        }
        segments.clear();
    }

    private File newSegmentFile(long segmentIdx, int level) {
        String name = segmentFileName(segmentIdx, level, extension);
        return new File(root, name);
    }

    public static String segmentFileName(long segmentIdx, int level, String ext) {
        if (segmentIdx < 0 || level < 0) {
            throw new RuntimeException("Invalid segment values, level: " + level + ", idx: " + segmentIdx);
        }
        String fileLevel = toLevelString(level);
        String fileLevelIdx = String.format("%0" + BitUtil.decimalUnitsForDecimal(Long.MAX_VALUE) + "d", segmentIdx);
        return fileLevel + "-" + fileLevelIdx + "." + ext;
    }

    private static String toLevelString(int level) {
        return String.format("%0" + BitUtil.decimalUnitsForDecimal(Integer.MAX_VALUE) + "d", level);
    }

    private boolean matchExtension(File file) {
        return file.getName().endsWith(extension);
    }

    //------------------------

    private static <T extends SegmentFile> long segmentIdx(T sf) {
        return Long.parseLong(name(sf.file()).split(SEPARATOR)[1]);
    }

    private static String name(File file) {
        return file.getName().split("\\.")[0];
    }

    private static <T extends SegmentFile> int level(T sf) {
        return Integer.parseInt(name(sf.file()).split(SEPARATOR)[0]);
    }

    public static class MergeFile<T extends SegmentFile> {
        private final T replacement;
        private final List<T> sources;

        private MergeFile(T replacement, List<T> sources) {
            this.replacement = replacement;
            this.sources = sources;
        }

        public void discard() {

        }

    }

}
