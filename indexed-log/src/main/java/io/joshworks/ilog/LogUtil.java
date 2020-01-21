package io.joshworks.ilog;

import java.io.File;
import java.util.Comparator;

import static java.lang.String.format;

public class LogUtil {

    static final int LEVEL_DIGITS = 3;
    static final String EXT = ".log";
    static final int SEG_IDX_DIGITS = (int) (Math.log10(Long.MAX_VALUE) + 1);

    static File segmentFile(long segmentIdx, int level) {
        String name = format("%0" + SEG_IDX_DIGITS + "d", segmentIdx) + "-" + format("%0" + LEVEL_DIGITS + "d", level) + EXT;
        return new File(root, name);
    }

    static long segmentIdx(String fileName) {
        String name = nameWithoutExt(fileName);
        return Long.parseLong(name.split("-")[0]);
    }

    static int levelOf(String fileName) {
        String name = nameWithoutExt(fileName);
        return Integer.parseInt(name.split("-")[1]);
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
