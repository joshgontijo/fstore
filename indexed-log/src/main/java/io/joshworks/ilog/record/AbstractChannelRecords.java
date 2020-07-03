package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

abstract class AbstractChannelRecords extends AbstractRecords {

    private StripedBufferPool pool;
    private BufferRecords records;
    private ByteBuffer readBuffer;
    private boolean closed;

    AbstractChannelRecords(String poolName) {
        super(poolName);
    }

    protected void init(int bufferSize, StripedBufferPool pool) {
        this.pool = pool;
        this.readBuffer = pool.allocate(bufferSize);
    }

    protected abstract int read(ByteBuffer readBuffer) throws IOException;

    private int readBatch() {
        if (closed) {
            return -1;
        }
        if (records != null) {
            records.close();
        }
        try {
            int read = read(readBuffer);
            if (read == -1) {
                close();
                return read;
            }
            if (read == 0) {
                return 0;
            }
            readBuffer.flip();
            records = RecordsPool.fromBuffer(poolName(), readBuffer);
            readBuffer.compact();

            return read;
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
    public boolean hasNext() {
        return peek() != null;
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
        pool.free(readBuffer);
    }

}
