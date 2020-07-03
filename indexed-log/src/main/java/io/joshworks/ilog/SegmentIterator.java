package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordsPool;
import io.joshworks.ilog.record.BufferRecords;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class SegmentIterator implements Iterators.CloseableIterator<Record2> {

    private final ByteBuffer readBuffer;
    private final IndexedSegment segment;
    private final BufferPool pool;
    private long readPos;
    private int recordsIdx = 0;
    private BufferRecords records = RecordsPool.get("TODO - DEFINE");;

    public SegmentIterator(IndexedSegment segment, long startPos, BufferPool pool) {
        this.pool = pool;
        this.segment = segment;
        this.readPos = startPos;
        this.readBuffer = pool.allocate();
    }

    @Override
    public boolean hasNext() {
        if(recordsIdx >= records.size())
        if (hasNext(readBuffer)) {
            return true;
        }
        readBatch();
        return hasNext(readBuffer);
    }

    @Override
    public Record2 next() {
        readBuffer.limit(bufferLimit).position(bufferPos);
        if (!hasNext(readBuffer)) {
            throw new NoSuchElementException();
        }

        bufferPos += Record.sizeOf(readBuffer);
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
        int rsize = Record.sizeOf(record);
        return rsize <= remaining && rsize > HEADER_BYTES;
    }

    private void readBatch() {
        if (readPos + bufferPos >= segment.writePosition()) {
            return;
        }

        int read = segment.read(readPos, readBuffer);
        if (read <= 0) {
            return;
        }
        records.close();
        records = RecordsPool.get("TODO - DEFINE");
        recordsIdx = 0;

        readPos += read;
        readBuffer.flip();
        records.fromBuffer(readBuffer);
        readBuffer.compact();
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
