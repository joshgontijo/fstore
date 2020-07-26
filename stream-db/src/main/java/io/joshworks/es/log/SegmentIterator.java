package io.joshworks.es.log;

import io.joshworks.es.Event;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.fstore.core.util.Memory;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static io.joshworks.es.Event.OVERHEAD;

public class SegmentIterator implements Iterators.CloseableIterator<ByteBuffer> {

    private ByteBuffer readBuffer;
    private final int bufferSize;
    private final LogSegment segment;
    private long readPos;
    private int bufferPos;
    private int bufferLimit;

    SegmentIterator(LogSegment segment, long startPos, int bufferSize) {
        if (bufferSize < Memory.PAGE_SIZE) {
            throw new IllegalArgumentException("Buffer size must be at least " + Memory.PAGE_SIZE);
        }
        this.segment = segment;
        this.readPos = startPos;
        this.bufferSize = bufferSize;
    }

    @Override
    public boolean hasNext() {
        tryAllocateBuffer();
        readBuffer.limit(bufferLimit).position(bufferPos);
        if (hasNext(readBuffer)) {
            return true;
        }
        readBatch();
        return hasNext(readBuffer);
    }

    @Override
    public ByteBuffer next() {
        tryAllocateBuffer();
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
        if (remaining < OVERHEAD) {
            return false;
        }
        int rsize = Event.sizeOf(record);
        return rsize <= remaining && rsize > OVERHEAD;
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

    public long address() {
        long segIdx = segment.segmentIdx();
        long logPos = readPos + bufferPos;
        return Log.toSegmentedPosition(segIdx, logPos);
    }

    public boolean endOfLog() {
        return segment.readOnly() && !hasReadableBytes() && !hasNext();
    }

    private boolean hasReadableBytes() {
        return segment.writePosition() - readPos > 0;
    }

    private void tryAllocateBuffer() {
        if (readBuffer == null) {
            readBuffer = Buffers.allocate(bufferSize, false);
            this.bufferLimit = readBuffer.limit();
        }
    }

    @Override
    public void close() {
        //do nothing
    }
}
