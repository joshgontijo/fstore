package io.joshworks.ilog;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordIterator;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.nio.ByteBuffer;

public class SegmentIterator implements Iterators.CloseableIterator<Record2> {

    private final ByteBuffer readBuffer;
    private final IndexedSegment segment;
    private final RecordPool pool;
    private final Records records;
    private RecordIterator recIt;
    private long readPos;

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
        if (recIt.hasNext()) {
            return true;
        }
        readBatch();
        return recIt.hasNext();
    }

    @Override
    public Record2 next() {
        if (!hasNext()) {
            return null;
        }
        return recIt.next();
    }

    public Record2 peek() {
        if (!hasNext()) {
            return null;
        }
        return recIt.peek();
    }


    private void readBatch() {
        if (readPos >= segment.writePosition()) {
            return;
        }
        long rpos = readPos;

        int read = segment.read(readBuffer, rpos);
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
        recIt = null;
        records.close();
        segment.release(this);
    }
}
