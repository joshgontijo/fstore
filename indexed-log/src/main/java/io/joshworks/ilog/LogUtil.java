package io.joshworks.ilog;

import io.joshworks.fstore.core.util.BitUtil;

import java.io.File;
import java.util.Comparator;

public class LogUtil {

    //    private static final long BASE = 0x16345785D8A0000L; //100000000000000000
    static final String EXT = ".log";
    static final int SEG_IDX_DIGITS = (int) (Math.log10(Long.MAX_VALUE) + 1);


    private static final int SEGMENT_BITS = 8;
    private static final int SEGMENT_ADDRESS_BITS = Long.SIZE - SEGMENT_BITS;

    static final long MAX_SEGMENTS = BitUtil.maxValueForBits(SEGMENT_BITS);
    static final long MAX_SEGMENT_IDX = BitUtil.maxValueForBits(SEGMENT_ADDRESS_BITS);

    static File segmentFile(File root, long segmentIdx, int level) {
        String name = segmentFileName(segmentIdx, level);
        return new File(root, name);
    }

    public static String segmentFileName(long segmentIdx, int level) {
        if (segmentIdx < 0 || level < 0) {
            throw new RuntimeException("Invalid segment values");
        }
        long id = toSegmentId(level, segmentIdx);
        return String.format("%0" + SEG_IDX_DIGITS + "d", id) + EXT;
    }

    static long segmentId(String fileName) {
        String name = nameWithoutExt(fileName);
        return Long.parseLong(name);
    }

    private static long toSegmentId(long level, long segmentIdx) {
        if (level < 0) {
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (level > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + MAX_SEGMENTS);
        }
        return (level << SEGMENT_ADDRESS_BITS) | segmentIdx;
    }

    static long segmentIdx(String fileName) {
        long segmentId = segmentId(fileName);
        long mask = (1L << SEGMENT_ADDRESS_BITS) - 1;
        long segIdx = (segmentId & mask);
        if (segIdx > MAX_SEGMENT_IDX) {
            throw new IllegalStateException("Segment index is greater than max value of " + MAX_SEGMENT_IDX);
        }
        return segIdx;
    }

    static int levelOf(String fileName) {
        long segmentId = segmentId(fileName);
        long segmentIdx = (segmentId >>> SEGMENT_ADDRESS_BITS);
        if (segmentIdx > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Invalid segment, value cannot be greater than " + MAX_SEGMENTS);
        }
        return (int) segmentIdx;
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
                int createdDiff = Long.compare(o1.segmentId(), o2.segmentId());
                if (createdDiff != 0)
                    return createdDiff;
            }
            return levelDiff;
        };
    }
}
