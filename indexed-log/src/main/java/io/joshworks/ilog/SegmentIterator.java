package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.ilog.record.Record;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static io.joshworks.ilog.record.Record.HEADER_BYTES;


public class SegmentIterator implements Iterators.CloseableIterator<ByteBuffer> {

    private final ByteBuffer readBuffer;
    private final Segment segment;
    private final BufferPool pool;
    private long readPos;
    private int bufferPos;
    private int bufferLimit;

    public SegmentIterator(Segment segment, long startPos, BufferPool pool) {
        this.pool = pool;
        this.segment = segment;
        this.readPos = startPos;
        this.readBuffer = pool.allocate();
        this.bufferLimit = readBuffer.limit();
    }

    @Override
    public boolean hasNext() {
        readBuffer.limit(bufferLimit).position(bufferPos);
        if (hasNext(readBuffer)) {
            return true;
        }
        readBatch();
        return hasNext(readBuffer);
    }

    @Override
    public ByteBuffer next() {
        readBuffer.limit(bufferLimit).position(bufferPos);
        if (!hasNext(readBuffer)) {
            throw new NoSuchElementException();
        }

        assert Record.isValid(readBuffer);

        bufferPos += Record.size(readBuffer);
        return readBuffer;
    }

    public ByteBuffer peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return readBuffer;
    }

    private boolean hasNext(ByteBuffer record) {
        int remaining = record.remaining();
        if (remaining < HEADER_BYTES) {
            return false;
        }
        int rsize = Record.size(record);
        return rsize <= remaining && rsize > HEADER_BYTES;
    }

    private void readBatch() {
        if (readPos + bufferPos >= segment.writePosition()) {
            return;
        }
        readPos += bufferPos;
        long rpos = readPos;
        if (readBuffer.position() > 0) {
            readBuffer.compact();
            rpos += readBuffer.position();
        }

        int read = segment.read(readBuffer, rpos);
        if (read <= 0) { //EOF or no more data
            throw new IllegalStateException("Expected data to be read");
        }
        readBuffer.flip();
        bufferPos = 0;
        bufferLimit = readBuffer.limit();
    }

    //internal
    long position() {
        return readPos;
    }

    public boolean endOfLog() {
        return segment.readOnly() && !hasReadableBytes() && !hasNext();
    }

    private boolean hasReadableBytes() {
        return segment.writePosition() - readPos > 0;
    }

    @Override
    public void close() {
        pool.free(readBuffer);
        segment.release(this);
    }
}
