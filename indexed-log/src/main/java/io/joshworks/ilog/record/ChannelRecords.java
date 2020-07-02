package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;

public class ChannelRecords extends AbstractRecords {

    private ReadableByteChannel src;
    private StripedBufferPool pool;
    private BufferRecords records;
    private ByteBuffer readBuffer;
    private boolean closed;

    ChannelRecords(String poolName) {
        super(poolName);
    }

    void init(ReadableByteChannel src, int bufferSize, StripedBufferPool pool) {
        this.src = src;
        this.pool = pool;
        this.readBuffer = pool.allocate(bufferSize);
    }

    private int readBatch() {
        if (closed) {
            return -1;
        }
        if (records != null) {
            records.close();
        }
        try {
            int read = src.read(readBuffer);
            if (read == -1) {
                close();
                return read;
            }
            if (read == 0) {
                return 0;
            }
            readBuffer.flip();
            records = RecordPool.fromBuffer(poolName(), readBuffer);
            readBuffer.compact();

            return records.totalSize();
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read from channel", e);
        }
    }

    @Override
    public Record2 poll() {
        if (records == null || records.peek() == null) {
            readBatch();
        }
        return records == null ? null : records.poll();
    }

    @Override
    public Record2 peek() {
        Record2 rec;
        if (records == null || (rec = records.peek()) == null) {
            readBatch();
            rec = records == null ? null : records.peek();
        }
        return rec;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) {
        long written = 0;
        while (readBatch() > 0) {
            written += records.writeTo(channel);
        }
        return written;
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int count) {
        long written = 0;
        int items = 0;
        if (records != null) {
            while (records.peek() != null && items < count) {
                written += records.writeTo(channel);
                items++;
            }
        }
        return written;
    }


    @Override
    public void close() {
        closed = true;
        src = null;
        pool.free(readBuffer);
    }

}
