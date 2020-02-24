package io.joshworks.ilog;

import java.nio.ByteBuffer;

public class LogIterator {

    private final Log<?> log;
    public long segPos;
    public int segIdx;

    public LogIterator(Log<?> log) {
        this.log = log;
    }

    public int read(ByteBuffer dst) {

        return log.view.apply(Direction.FORWARD, segs -> {
            if (segIdx >= segs.size()) {
                return 0;
            }
            IndexedSegment segment = segs.get(segIdx);
            int read = segment.read(segPos, dst);
            if (read == 0) {
                return 0;
            }
            if (read < 0) {
                if (segIdx + 1 >= segs.size()) {
                    return 0;
                }
                segIdx++;
                segPos = 0;
                segment = segs.get(segIdx);
                read = segment.read(segPos, dst);
            }
            if (read > 0) {
                segPos += read;
            }
            return read;
        });

    }
}
