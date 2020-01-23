package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Iterators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class RecordBatchIterator implements Iterators.CloseableIterator<ByteBuffer> {

    private final ByteBuffer readBuffer;
    private final IndexedSegment segment;
    private final BufferPool pool;
    private long readPos;

    public RecordBatchIterator(IndexedSegment segment, long startPos, BufferPool pool) {
        this.pool = pool;
        this.readBuffer = pool.allocate();
        this.segment = segment;
        this.readPos = startPos;
        if (readBuffer.capacity() < HEADER_BYTES) {
            pool.free(readBuffer);
            throw new IllegalArgumentException("Read buffer must be at least " + HEADER_BYTES);
        }
    }

    @Override
    public boolean hasNext() {
        if (RecordBatch.hasNext(readBuffer)) {
            return true;
        }
        readBatch();
        return RecordBatch.hasNext(readBuffer);
    }

    @Override
    public ByteBuffer next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return readBuffer;
    }

    public long transferTo(WritableByteChannel channel) throws IOException {
        long written = 0;
        while (hasNext() && !endOfLog()) {
            ByteBuffer buffer = next();
            int w = Record2.writeTo(buffer, channel);
            if (w > 0) written += w;
        }
        return written;
    }

    private void readBatch() {
        try {
            int position = readBuffer.position();
            readPos += position;
            if (position > 0) {
                readBuffer.compact();
                readPos += readBuffer.position();
            }
            if (!hasReadableBytes()) {
                return;
            }

            int read = segment.read(readPos, readBuffer);
            if (read <= 0) { //EOF or no more data
                return;
            }
            readBuffer.flip();
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to read batch", e);
        }
    }

    //internal
    long position() {
        return readPos;
    }

    public boolean endOfLog() {
        return segment.readOnly() && !hasReadableBytes() && !hasNext();
    }

    public long readableBytes() {
        return segment.writePosition() - readPos;
    }

    public boolean hasReadableBytes() {
        return readableBytes() > 0;
    }

    @Override
    public void close() {
        pool.free(readBuffer);
    }
}
