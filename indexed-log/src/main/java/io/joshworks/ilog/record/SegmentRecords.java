package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.ilog.IndexedSegment;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

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

    @Override
    public long writeTo(GatheringByteChannel channel) {
        try {
            if (records.hasNext()) { //flush remaining data from buffers
                return records.writeTo(channel);
            }
            if (readBuffer.hasRemaining()) {
                return channel.write(readBuffer);
            }

            //use sendFile
            long transferred = segment.channel().transferTo(readPos, readBuffer.capacity(), channel);
            readPos += transferred;

            return transferred;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to send data", e);
        }
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int count) {
        return super.writeTo(channel, count);
    }
}
