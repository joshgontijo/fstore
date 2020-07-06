package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
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
            int read = segment.channel().read(readBuffer, readPos);
            if (read == 0 && endOfLog()) {
                return -1;
            }
            readPos += read;
            return read;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read", e);
        }
    }

    @Override
    public long writeTo(GatheringByteChannel channel) {
        try {
            if (records.hasNext()) { //flush remaining data from buffers
                long written = records.writeTo(channel);
                return updateWritten(written);
            }
            if (readBuffer.hasRemaining()) {
                long written = channel.write(readBuffer);
                return updateWritten(written);
            }

            //use sendFile
            long transferred = segment.channel().transferTo(readPos, readBuffer.capacity(), channel);
            return updateWritten(transferred);

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

    private long updateWritten(long written) {
        readPos += written;
        return written;
    }
}
