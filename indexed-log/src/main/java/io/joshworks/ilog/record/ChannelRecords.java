package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;

class ChannelRecords extends AbstractRecords {

    protected Records records;
    protected ByteBuffer readBuffer;
    private boolean closed;
    private ReadableByteChannel src;

    ChannelRecords(RecordPool bufferPool) {
        super(bufferPool);
    }

    protected void init(int bufferSize, ReadableByteChannel src) {
        this.readBuffer = pool.allocate(bufferSize);
        this.records = pool.fromBuffer(readBuffer);
        this.src = src;
    }

    private int read(ByteBuffer readBuffer) throws IOException {
        return src.read(readBuffer);
    }

    private int readBatch() {
        if (closed) {
            return -1;
        }
        if (records != null) {
            if (records.hasNext()) {
                return 0;
            }
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
            records = pool.fromBuffer(readBuffer);
            readBuffer.compact();

            return read;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read from channel", e);
        }
    }

    @Override
    public Record2 poll() {
        if (hasNext()) {//using hasNext to trigger readBatch
            return records.poll();
        }
        return null;
    }

    @Override
    public Record2 peek() {
        Record2 rec;
        if ((rec = records.peek()) == null) {
            readBatch();
            rec = records.peek();
        }
        return rec;
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) {
        if (hasNext()) { //just to trigger read if buffer is empty
            return records.writeTo(channel);
        }
        return 0;
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int offset, int count) {
        if (hasNext()) { //just to trigger read if buffer is empty
            return records.writeTo(channel, offset, count);
        }
        return 0;
    }

    @Override
    public void close() {
        closed = true;
        pool.free(readBuffer);
        records.close();
    }

    @Override
    public int size() {
        return records.size();
    }

}
