package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.IndexedSegment;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class SegmentRecords extends AbstractChannelRecords {

    private IndexedSegment segment;
    private long readPos;

    SegmentRecords(RecordPool pool) {
        super(pool);
    }

    void init(int bufferSize, IndexedSegment segment, long startPos) {
        super.init(bufferSize);
        this.segment = segment;
        this.readPos = startPos;
    }

    @Override
    protected int read(ByteBuffer readBuffer) {
        try {
            if (endOfLog()) {
                return 0;
            }
            int read = segment.channel().read(readBuffer, readPos);
            readPos += read;
            return read;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read", e);
        }
    }

    @Override
    public long writeTo(GatheringByteChannel channel) {
        try {
            long total = 0;
            if (records.hasNext()) { //flush remaining data from buffers
                total += records.writeTo(channel);
            }
            if (readBuffer.position() > 0) { //flush incomplete records
                readBuffer.flip();
                Buffers.writeFully(channel, readBuffer);
                readBuffer.compact();
            }

            //use sendFile
            long transferred = segment.channel().transferTo(readPos, segment.size() - readPos, channel);
            if (transferred == -1) {
                return -1;
            }
            total += transferred;
            readPos += total;
            return total;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to send data", e);
        }
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int count) {
        return super.writeTo(channel, count);
    }

    @Override
    public boolean hasNext() {
        return readPos < segment.size();
    }

    public boolean endOfLog() {
        return !hasNext() && segment.readOnly();
    }

}
