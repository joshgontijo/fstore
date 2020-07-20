package io.joshworks.es.log;

import io.joshworks.es.SegmentDirectory;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.BitUtil;

import java.io.File;
import java.nio.ByteBuffer;

public class Log extends SegmentDirectory<LogSegment> {

    private static final int SEGMENT_BITS = 24;
    private static final int SEGMENT_ADDRESS_BITS = Long.SIZE - SEGMENT_BITS;
    private static final long MAX_SEGMENTS = BitUtil.maxValueForBits(SEGMENT_BITS);
    private static final long MAX_SEGMENT_ADDRESS = BitUtil.maxValueForBits(SEGMENT_ADDRESS_BITS);

    private static final String EXTENSION = "log";
    private final long logSize;

    public Log(File root, long logSize) {
        super(root, EXTENSION);
        this.logSize = logSize;
        openLogs();
    }

    public void openLogs() {
        loadSegments(LogSegment::open);
        if (!segments.isEmpty()) {
            LogSegment head = segments.get(segments.size() - 1);
            //TODO merged segment with higher idx will be used as head here, which is wrong
            //need to read header and check which one needs to be restored
            head.restore(BufferPool.unpooled(8192, false));
            head.forceRoll();
        }

        createNewHead();
    }

    public void createNewHead() {
        File file = newSegmentFile(0);
        LogSegment segment = LogSegment.create(file, logSize);
        segments.add(segment);
        makeHead(segment);
    }

    //TODO append should just go in EventStore when moving to appendMany because each entry needs its own address
    public long append(ByteBuffer data) {
        LogSegment head = head();
        int logIdx = head.segmentIdx();
        long logPos = head.append(data);
        return toSegmentedPosition(logIdx, logPos);
    }

    public int read(ByteBuffer dst, long address) {
        int segIdx = segmentIdx(address);
        long logPos = positionOnSegment(address);
        LogSegment segment = segments.get(segIdx);
        return segment.read(dst, logPos);
    }

    void roll() {
        LogSegment head = head();
        head.roll();
        createNewHead();
    }

    private static int segmentIdx(long address) {
        long segmentIdx = (address >>> SEGMENT_ADDRESS_BITS);
        if (segmentIdx > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Invalid segment, value cannot be greater than " + MAX_SEGMENTS);
        }

        return (int) segmentIdx;
    }

    private static long toSegmentedPosition(long segmentIdx, long position) {
        if (segmentIdx < 0) {
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (segmentIdx > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + MAX_SEGMENTS);
        }
        return (segmentIdx << SEGMENT_ADDRESS_BITS) | position;
    }

    private static long positionOnSegment(long address) {
        long mask = (1L << SEGMENT_ADDRESS_BITS) - 1;
        return (address & mask);
    }
}
