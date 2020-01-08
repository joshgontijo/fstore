package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class RecordBatchIterator implements SegmentIterator {

    protected final FileChannel channel;
    protected final AtomicLong writePosition;
    protected final long startPos;
    protected final AtomicLong readPos = new AtomicLong();
    private final ByteBuffer readBuffer;
    private final Queue<Record> records = new ArrayDeque<>();
    private long entriesRead;
    private Record next;

    public RecordBatchIterator(FileChannel channel, long startPos, AtomicLong writePosition, int batchSize) {
        if (batchSize < HEADER_BYTES) {
            throw new IllegalArgumentException("Batch size must be greater than " + HEADER_BYTES);
        }
        this.channel = channel;
        this.writePosition = writePosition;
        this.readPos.set(startPos);
        this.startPos = startPos;
        this.readBuffer = Buffers.allocate(batchSize, false);
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        if (records.isEmpty()) {
            readBatch(readPos.get());
        }
        next = records.poll();
        return next != null;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        readPos.addAndGet(next.recordLength());
        Record record = next;
        next = null;
        entriesRead++;
        return record;
    }

    private void readBatch(long position) {
        if (writePosition.get() <= readPos.get() + HEADER_BYTES) {
            return;
        }
        try {
            if (readBuffer.position() > 0) {
                readBuffer.compact();
            }
            int remainingBytes = readBuffer.position();
            int read = channel.read(readBuffer, position + remainingBytes);
            if (read <= 0) { //EOF or no more data
                return;
            }
            readBuffer.flip();
            Record record;
            do {
                record = Record.from(readBuffer, false);
                if (record != null) {
                    records.add(record);
                } else if (readBuffer.remaining() >= HEADER_BYTES) {
                    int recordLength = Record.recordLength(readBuffer);
                    if (recordLength > readBuffer.capacity()) {
                        long recordPos = position + readBuffer.position();
                        record = readLargerEntry(recordPos, recordLength);
                    }
                }
            } while (record != null);

        } catch (IOException e) {
            throw new RuntimeIOException("Failed to read batch", e);
        }
    }

    private Record readLargerEntry(long position, int recordLength) throws IOException {
        ByteBuffer buffer = Buffers.allocate(recordLength, false);
        int read = channel.read(buffer, position);
        if (read != recordLength) {
            throw new RuntimeIOException("Invalid entry at position " + position);
        }
        return Record.from(buffer, false);
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
        //TODO implement
    }
}
