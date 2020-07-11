package io.joshworks.ilog;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordIterator;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.nio.ByteBuffer;

public class SegmentIterator implements Iterators.CloseableIterator<Record> {

    private ByteBuffer readBuffer;
    private final IndexedSegment segment;
    private final RecordPool pool;
    private final Records records;
    private RecordIterator recIt;
    private long readPos;
    private boolean closed;

    public SegmentIterator(IndexedSegment segment, long startPos, int bufferSize, RecordPool pool) {
        this.pool = pool;
        this.segment = segment;
        this.readPos = startPos;
        this.readBuffer = pool.allocate(bufferSize);
        this.records = pool.empty();
        this.recIt = records.iterator();
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        if (recIt.hasNext()) {
            return true;
        }
        readBatch();
        boolean hasNext = recIt.hasNext();
        if (!hasNext && endOfLog()) {
            close();
        }
        return hasNext;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            return null;
        }
        return recIt.next();
    }

    public Record peek() {
        if (!hasNext()) {
            return null;
        }
        return recIt.peek();
    }


    private void readBatch() {
        if (readPos >= segment.writePosition()) {
            return;
        }
        int read = segment.read(readBuffer, readPos);
        if (read == Storage.EOF) { //EOF or no more data
            throw new IllegalStateException("Unexpected EOF");
        }
        readBuffer.flip();

        records.clear();
        records.add(readBuffer);
        readBuffer.compact();
        readPos += read;
        recIt = records.iterator(); //clear already resets idx, but just to be explicit
    }

    public boolean endOfLog() {
        return segment.readOnly() && !hasReadableBytes();
    }

    private boolean hasReadableBytes() {
        return segment.writePosition() - readPos > 0;
    }

    @Override
    public void close() {
        closed = true;
        pool.free(readBuffer);
        readBuffer = null;
        recIt = null;
        records.close();
        segment.release(this);
    }
}
