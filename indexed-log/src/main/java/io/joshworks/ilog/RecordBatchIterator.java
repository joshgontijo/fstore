package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Iterators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class RecordBatchIterator implements Iterators.CloseableIterator<Record> {

    private final long startPos;
    private final AtomicLong readPos = new AtomicLong();
    private final ByteBuffer readBuffer;
    private final Queue<Record> records = new ArrayDeque<>();
    private final IndexedSegment segment;
    private final BufferPool pool;
    private long entriesRead;
    private Record next;

    public RecordBatchIterator(IndexedSegment segment, long startPos, BufferPool pool) {
        this.pool = pool;
        this.readBuffer = pool.allocate();
        this.segment = segment;
        this.startPos = startPos;
        this.readPos.set(startPos);
        if (readBuffer.capacity() < HEADER_BYTES) {
            pool.free(readBuffer);
            throw new IllegalArgumentException("Read buffer must be at least " + HEADER_BYTES);
        }
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        if (records.isEmpty()) {
            readBatch();
        }
        next = records.poll();
        return next != null;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        readPos.addAndGet(next.size());
        Record record = next;
        next = null;
        entriesRead++;
        return record;
    }

    private void readBatch() {
        try {
            if (readBuffer.position() > 0) {
                readBuffer.compact();
            }
            long rpos = readPos.get() + readBuffer.position();
            int read = segment.read(rpos, readBuffer);
            if (read <= 0) { //EOF or no more data
                return;
            }
            readBuffer.flip();
            Record record;
            while ((record = Record.from(readBuffer, false)) != null) {
                records.add(record);
            }
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to read batch", e);
        }
    }

    public long entriesRead() {
        return entriesRead;
    }

    public long bytesRead() {
        return readPos.get() - startPos;
    }

    //internal
    long position() {
        return readPos.get();
    }

    @Override
    public void close() {
        pool.free(readBuffer);
    }
}
