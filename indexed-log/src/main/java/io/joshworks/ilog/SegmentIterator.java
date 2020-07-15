package io.joshworks.ilog;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

public class SegmentIterator implements Iterators.CloseableIterator<Record> {

    private ByteBuffer readBuffer;
    private final Segment segment;
    private final RecordPool pool;
    private final Queue<Record> records = new ArrayDeque<>();
    private long readPos;
    private boolean closed;

    SegmentIterator(Segment segment, long startPos, int bufferSize, RecordPool pool) {
        this.pool = pool;
        this.segment = segment;
        this.readPos = startPos;
        this.readBuffer = pool.allocate(bufferSize);
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        if (!records.isEmpty()) {
            return true;
        }
        readBatch();
        boolean hasNext = !records.isEmpty();
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
        return records.poll();
    }

    public Record peek() {
        if (!hasNext()) {
            return null;
        }
        return records.peek();
    }


    private void readBatch() {
        assert records.isEmpty();

        if (readPos >= segment.writePosition()) {
            return;
        }
        int read = segment.read(readBuffer, readPos);
        if (read == Storage.EOF) { //EOF or no more data
            throw new IllegalStateException("Unexpected EOF");
        }
        readBuffer.flip();

        int copied = parseRecords(readBuffer);
        if (copied == 0 && Record.hasHeaderData(readBuffer) && Record.recordSize(readBuffer) > readBuffer.capacity()) {
            Record largeRec = readLargeEntry();
            records.add(largeRec);
        }
        readBuffer.compact();
        readPos += read;
    }

    private int parseRecords(ByteBuffer src) {
        int copied = 0;
        Record rec;
        do {
            rec = pool.from(src);
            if (rec != null) {
                records.add(rec);
                copied += rec.recordSize();
            }
        } while (rec != null);
        return copied;
    }

    public boolean endOfLog() {
        return segment.readOnly() && !hasReadableBytes();
    }

    private boolean hasReadableBytes() {
        return segment.writePosition() - readPos > 0;
    }

    private Record readLargeEntry() {
        assert Record.hasHeaderData(readBuffer);

        int recSize = Record.recordSize(readBuffer);
        assert recSize > readBuffer.capacity();

        ByteBuffer recBuffer = pool.allocate(recSize);
        Buffers.copy(readBuffer, recBuffer); //copy data to bigger buffer to avoid re-reading
        try {
            int read = segment.read(recBuffer, readPos); //read remaining recordSize
            assert read == recBuffer.limit();
            recBuffer.flip();

            Record rec = pool.from(recBuffer, recBuffer.position());
            assert rec != null;
            return rec;
        } finally {
            pool.free(recBuffer);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        pool.free(readBuffer);
        readBuffer = null;
        segment.release(this);
    }
}