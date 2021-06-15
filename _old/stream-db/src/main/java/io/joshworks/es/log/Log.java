package io.joshworks.es.log;

import io.joshworks.es.Event;
import io.joshworks.es.SegmentDirectory;
import io.joshworks.fstore.core.util.BitUtil;
import io.joshworks.fstore.core.util.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class Log extends SegmentDirectory<LogSegment> {

    private static final Logger logger = LoggerFactory.getLogger(Log.class);

    public static final int START = 0;

    private static final int SEGMENT_BITS = 24;
    private static final int SEGMENT_ADDRESS_BITS = Long.SIZE - SEGMENT_BITS;
    private static final long MAX_SEGMENTS = BitUtil.maxValueForBits(SEGMENT_BITS);
    private static final long MAX_SEGMENT_ADDRESS = BitUtil.maxValueForBits(SEGMENT_ADDRESS_BITS);

    private static final String EXTENSION = "log";
    private final long logSize;

    public Log(File root, long logSize) {
        super(root, EXTENSION, MAX_SEGMENTS);
        logger.info("MAX_SEGMENTS: {}", MAX_SEGMENTS);
        logger.info("MAX_SEGMENT_ADDRESS: {}", MAX_SEGMENT_ADDRESS);
        this.logSize = logSize;
        this.openLogs();
    }

    public long restore(long address, BiConsumer<Long, ByteBuffer> handler) {
        long segIdx = segmentIdx(address);
        long logPos = positionOnSegment(address);

        LogSegment first = getSegment(segIdx);
        SegmentIterator it = first.iterator(Memory.PAGE_SIZE, logPos);
        long entries = processEntries(handler, it);

        for (long idx = segIdx + 1; idx < headIdx(); idx++) {
            LogSegment segment = tryGet(idx);
            if (segment == null) {
                continue;
            }
            entries += processEntries(handler, segment.iterator());
        }
        return entries;
    }

    public long processEntries(BiConsumer<Long, ByteBuffer> handler, SegmentIterator it) {
        long count = 0;
        while (it.hasNext()) {
            long recAddress = it.address();
            ByteBuffer bb = it.next();
            handler.accept(recAddress, bb);
            count++;
        }
        return count;
    }

    private synchronized void openLogs() {
        loadSegments(LogSegment::open);
        if (!super.isEmpty()) {
            LogSegment head = head();
            head.restore(Event::isValid);
            head.forceRoll();
        }

        createNewHead();
    }

    public void createNewHead() {
        File file = newHeadFile();
        LogSegment segment = LogSegment.create(file, logSize);
        super.add(segment);
    }

    public long append(ByteBuffer data) {
        LogSegment head = head();
        long logIdx = head.segmentIdx();
        long logPos = head.append(data);
        return toSegmentedPosition(logIdx, logPos);
    }

    public boolean full() {
        return head().writePosition() >= logSize;
    }

    public long segmentIdx() {
        return head().segmentIdx();
    }

    public long segmentPosition() {
        return head().writePosition();
    }

    public int read(long address, ByteBuffer dst) {
        int segIdx = segmentIdx(address);
        long logPos = positionOnSegment(address);

        LogSegment segment = getSegment(segIdx);
        return segment.read(dst, logPos);
    }

    public void roll() {
        LogSegment head = head();
        head.roll();
        createNewHead();
    }

    public List<LogSegment> compactionSegments(int level) {
        List<LogSegment> segments = new ArrayList<>();
        //everything except head, needs to be changed to add replicated position
        for (long idx = 0; idx < headIdx(); idx++) {
            LogSegment segment = tryGet(idx);
            if (segment == null || segment.level() != level) {
                continue;
            }
            segments.add(segment);
        }
        return segments;
    }

    //---------

    public static int segmentIdx(long address) {
        long segmentIdx = (address >>> SEGMENT_ADDRESS_BITS);
        if (segmentIdx > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Invalid segment, value cannot be greater than " + MAX_SEGMENTS);
        }

        return (int) segmentIdx;
    }

    public static long toSegmentedPosition(long segmentIdx, long position) {
        if (segmentIdx < 0) {
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (segmentIdx > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + MAX_SEGMENTS);
        }
        return (segmentIdx << SEGMENT_ADDRESS_BITS) | position;
    }

    public static long positionOnSegment(long address) {
        long mask = (1L << SEGMENT_ADDRESS_BITS) - 1;
        return (address & mask);
    }


}
