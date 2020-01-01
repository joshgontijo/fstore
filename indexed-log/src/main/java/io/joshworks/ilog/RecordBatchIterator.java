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

import static io.joshworks.ilog.RecordHeader.HEADER_BYTES;

public class RecordBatchIterator extends RecordIterator {

    private final ByteBuffer readBuffer;
    private final Queue<Record> records = new ArrayDeque<>();
    private Record next;

    public RecordBatchIterator(FileChannel channel, long startOffset, long startPos, AtomicLong writePosition, int batchSize) {
        super(channel, startOffset, startPos, writePosition);
        if (batchSize < HEADER_BYTES) {
            throw new IllegalArgumentException("Batch size must be greater than " + HEADER_BYTES);
        }
        this.lastOffset.set(startOffset - 1);
        this.readPos.set(startPos);
        this.readBuffer = Buffers.allocate(batchSize, false);
    }

    @Override
    public boolean hasNext() {
        if (records.isEmpty() && next == null) {
            readBatch(readPos.get());
        }
        if (next == null) {
            next = takeWhile();
        }
        return next != null;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        readPos.addAndGet(next.size());
        Record r = next;
        next = null;
        return checkAndUpdateOffset(r);
    }

    private Record takeWhile() {
        Record record;
        while ((record = records.poll()) != null && record.offset < startOffset) {
            readPos.addAndGet(record.size());
        }
        return record;
    }

    private void readBatch(long position) {
        if (writePosition.get() <= readPos.get() + HEADER_BYTES) {
            return;
        }
        try {
            int remainingBytes = readBuffer.position();
            int read = channel.read(readBuffer, position + remainingBytes);
            if (read < HEADER_BYTES) {
                throw new RuntimeIOException("Invalid entry at position " + position);
            }
            readBuffer.flip();
            while (readBuffer.hasRemaining()) {
                if (readBuffer.remaining() < HEADER_BYTES) {
                    readBuffer.compact();
                    return;
                }
                RecordHeader header = RecordHeader.parse(readBuffer);
                if (header.length > readBuffer.remaining()) {
                    if (header.length > readBuffer.capacity()) { //record will never fit, read with a one off buffer
                        Record record = readLargerEntry(position, header);
                        records.add(record);
                    } else {
                        //rewinds header position which was read and not used
                        readBuffer.position(readBuffer.position() - HEADER_BYTES);
                    }
                    readBuffer.compact();
                    return;
                }
                Record record = Record.from(readBuffer, header, true);
                records.add(record);
            }
            readBuffer.compact();
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to read batch", e);
        }
    }


    private Record readLargerEntry(long position, RecordHeader header) throws IOException {
        ByteBuffer buffer = Buffers.allocate(header.length, false);
        int read = channel.read(buffer, position);
        if (read != header.length) {
            throw new RuntimeIOException("Invalid entry at position " + position);
        }
        return Record.from(buffer, header, false);

    }
}
