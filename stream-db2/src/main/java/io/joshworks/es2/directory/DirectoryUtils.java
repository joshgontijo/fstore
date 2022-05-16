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
        return new SegmentId(level, segmentIdx) + "." + ext;
    }

    //------------------------

    static SegmentId segmentId(File file) {
        String id = file.getName().split("\\.")[0];
        return SegmentId.from(id);
    }

    static <T extends SegmentFile> SegmentId segmentId(T sf) {
        return SegmentId.from(sf.name());
    }


}
