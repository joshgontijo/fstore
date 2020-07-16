package io.joshworks.ilog;

import java.io.File;
import java.util.Comparator;

public class LogUtil {

    //    private static final long BASE = 0x16345785D8A0000L; //100000000000000000
    static final String EXT = ".log";
    static final int SEG_IDX_DIGITS = (int) (Math.log10(Long.MAX_VALUE) + 1);


    static File segmentFile(File root, long segmentIdx, int level) {
        String name = segmentFileName(segmentIdx, level);
        return new File(root, name);
    }

    public static String segmentFileName(long segmentIdx, int level) {
        if (segmentIdx < 0 || level < 0) {
            throw new RuntimeException("Invalid segment values, level: " + level + ", idx: " + segmentIdx);
        }
        return String.format("%02d", level) + "-" + String.format("%0" + SEG_IDX_DIGITS + "d", segmentIdx) + EXT;
    }

    static long segmentIdx(String fileName) {
        return Long.parseLong(nameWithoutExt(fileName).split("-")[1]);
    }

    static int levelOf(String fileName) {
        return Integer.parseInt(fileName.split("-")[0]);
    }

    static File indexFile(File segmentFile) {
        String name = nameWithoutExt(segmentFile.getName());
        File dir = segmentFile.toPath().getParent().toFile();
        return new File(dir, name + ".index");
    }

    static String nameWithoutExt(String fileName) {
        return fileName.split("\\.")[0];
    }

    static Comparator<Segment> compareSegments() {
        return (o1, o2) -> {
            int levelDiff = o2.level() - o1.level();
            if (levelDiff == 0) {
                int createdDiff = Long.compare(o1.segmentIdx(), o2.segmentIdx());
                if (createdDiff != 0)
                    return createdDiff;
            }
            return levelDiff;
        };
    }
}
