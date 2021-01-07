package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.BitUtil;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

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

    static void deleteAllWithExtension(File root, String extension) {

        if (!root.exists()) {
            return;
        }
        if (!root.isDirectory()) {
            throw new IllegalArgumentException("Not a directory " + root.getAbsolutePath());
        }

        String[] names = root.list();
        if (names != null) {
            for (String s : names) {
                if (s.endsWith("\\." + extension)) {
                    Path path = root.toPath().resolve(s);
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        throw new RuntimeIOException("Failed to delete: " + path, e);
                    }
                }
            }
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

    static <T extends SegmentFile> long segmentIdx(T sf) {
        return Long.parseLong(sf.name().split(SEPARATOR)[1]);
    }

    static <T extends SegmentFile> int level(T sf) {
        return Integer.parseInt(sf.name().split(SEPARATOR)[0]);
    }

}
