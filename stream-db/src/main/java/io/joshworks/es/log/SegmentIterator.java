package io.joshworks.es.log;

import io.joshworks.es.Event;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Iterators;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static io.joshworks.es.Event.HEADER_BYTES;

public class SegmentIterator implements Iterators.CloseableIterator<ByteBuffer> {

    private final ByteBuffer readBuffer;
    private final LogSegment segment;
    private final BufferPool pool;
    private long readPos;
    private int bufferPos;
    private int bufferLimit;

    public SegmentIterator(LogSegment segment, long startPos, BufferPool pool) {
        this.pool = pool;
        this.segment = segment;
        this.readPos = startPos;
        this.readBuffer = pool.allocate();
        this.bufferLimit = readBuffer.limit();
        if (readBuffer.capacity() < HEADER_BYTES) {
            pool.free(readBuffer);
            throw new IllegalArgumentException("Read buffer must be at least " + HEADER_BYTES);
        }
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

        assert Event.isValid(readBuffer);

        bufferPos += Event.sizeOf(readBuffer);
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
        int rsize = Event.sizeOf(record);
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
