package io.joshworks.es.log;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Log {

    private final Map<Integer, LogSegment> segments = new ConcurrentHashMap<>();
    private LogSegment head;

    //TODO append should just go in EventStore when moving to appendMany because each entry needs its own address
    public long append(ByteBuffer data) {
        int segIdx = segments.size() - 1;
        long logPos = head.append(data);
        return toLogAddress(segIdx, logPos);
    }

    public int read(ByteBuffer dst, long address) {
        int segIdx = segmentIdx(address);
        long logPos = logPos(address);
        LogSegment segment = segments.get(segIdx);

        return segment.read(dst, logPos);
    }

    private long toLogAddress(int segIdx, long logPos) {
        return -1;
    }

    private int segmentIdx(long address) {
        return -1;
    }

    private long logPos(long address) {
        return -1;
    }

}
