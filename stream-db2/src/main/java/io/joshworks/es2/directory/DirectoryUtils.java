package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.util.BitUtil;

import java.io.File;

class DirectoryUtils {

    private static final String SEPARATOR = "-";

    public static String segmentFileName(long segmentIdx, int level, String ext) {
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
        return Long.parseLong(name(sf.name()).split(SEPARATOR)[1]);
    }

    static String name(File file) {
        return file.getName().split("\\.")[0];
    }

    static <T extends SegmentFile> int level(T sf) {
        return Integer.parseInt(name(sf.name()).split(SEPARATOR)[0]);
    }

}
