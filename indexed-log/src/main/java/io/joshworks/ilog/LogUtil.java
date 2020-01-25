package io.joshworks.ilog;

import java.io.File;
import java.util.Comparator;

import static java.lang.String.format;

public class LogUtil {

    private static long BASE = 0x16345785D8A0000L;
    static final String EXT = ".log";
    static final int SEG_IDX_DIGITS = (int) (Math.log10(Long.MAX_VALUE) + 1);

    static File segmentFile(File root, long segmentIdx, int level) {
        String name = segmentFileName(segmentIdx, level);
        return new File(root, name);
    }

    public static String segmentFileName(long segmentIdx, int level) {
        long id = (level * BASE) + segmentIdx;
        return format("%0" + SEG_IDX_DIGITS + "d", id) + EXT;
    }

    static long segmentId(String fileName) {
        String name = nameWithoutExt(fileName);
        return Long.parseLong(name);
    }

    static long segmentIdx(String fileName) {
        return segmentIdx(segmentId(fileName));
    }

    static long segmentIdx(long segmentId) {
        return segmentId - BASE;
    }

    static int levelOf(String fileName) {
        return levelOf(segmentId(fileName));
    }

    static int levelOf(long segmentId) {
        return (int) (segmentId / BASE);
    }

    static File indexFile(File segmentFile) {
        String name = nameWithoutExt(segmentFile.getName());
        File dir = segmentFile.toPath().getParent().toFile();
        return new File(dir, name + ".index");
    }

    static String nameWithoutExt(String fileName) {
        return fileName.split("\\.")[0];
    }

    static Comparator<IndexedSegment> compareSegments() {
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


}
