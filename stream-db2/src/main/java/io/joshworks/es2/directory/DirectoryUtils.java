package io.joshworks.es2.directory;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.BitUtil;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

class DirectoryUtils {

    private static final String SEPARATOR = "-";

    static void initDirectory(File root) {
        try {
            Files.createDirectories(root.toPath());
            if (!root.isDirectory()) {
                throw new IllegalArgumentException("Not a directory " + root.getAbsolutePath());
            }
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize segment directory", e);
        }
    }

    static Stream<Path> listMatchingExtension(File root, String extension) {
        if (!root.exists()) {
            return Stream.empty();
        }
        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory " + root.getAbsolutePath());
        }
        String[] names = root.list();
        if (names == null) {
            return Stream.empty();
        }
        return Arrays.stream(names)
                .filter(path -> path.endsWith("\\." + extension))
                .map(path -> root.toPath().resolve(path));
    }


    static void deleteAllWithExtension(File root, String extension) {
        listMatchingExtension(root, extension)
                .forEach(DirectoryUtils::delete);
    }

    private static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to delete: " + path, e);
        }
    }

    static String segmentFileName(long segmentIdx, int level, String ext) {
        if (segmentIdx < 0 || level < 0) {
            throw new RuntimeException("Invalid segment values, level: " + level + ", idx: " + segmentIdx);
        }
        String fileLevel = toLevelString(level);
        String fileLevelIdx = String.format("%0" + BitUtil.decimalUnitsForDecimal(Long.MAX_VALUE) + "d", segmentIdx);
        return fileLevel + "-" + fileLevelIdx + "." + ext;
    }

    static String toLevelString(int level) {
        return String.format("%0" + BitUtil.decimalUnitsForDecimal(Integer.MAX_VALUE) + "d", level);
    }


    //------------------------

    static <T extends SegmentFile> SegmentId segmentId(File file) {
        String id = file.getName().split("\\.")[0];
        return segmentId(id);
    }

    static <T extends SegmentFile> SegmentId segmentId(T sf) {
        return segmentId(sf.name());
    }

    static SegmentId segmentId(String id) {
        String[] part = id.split(SEPARATOR);
        int level = Integer.parseInt(part[0]);
        long idx = Long.parseLong(part[1]);
        return new SegmentId(level, idx);
    }
}
