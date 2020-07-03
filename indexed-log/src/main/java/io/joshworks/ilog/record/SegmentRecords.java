package io.joshworks.ilog.record;

import io.joshworks.ilog.IndexedSegment;

import java.nio.ByteBuffer;

public class SegmentRecords extends AbstractChannelRecords {

    private IndexedSegment segment;
    private long readPos;

    SegmentRecords(String poolName) {
        super(poolName);
    }

    void init(int bufferSize, StripedBufferPool pool, IndexedSegment segment, long startPos) {
        super.init(bufferSize, pool);
        this.segment = segment;
        this.readPos = startPos;
    }

    @Override
    protected int read(ByteBuffer readBuffer) {
        int read = segment.read(readPos, readBuffer);
        readPos += read;
        return read;
    }
}
