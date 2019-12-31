package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import static io.joshworks.ilog.RecordHeader.HEADER_BYTES;

public class RecordIterator implements Iterator<Record> {

    private final FileChannel channel;
    private final long startOffset;
    private final AtomicLong writePosition;
    private final long startPos;
    private final AtomicLong readPos = new AtomicLong();
    private final AtomicLong lastOffset = new AtomicLong();

    private final ByteBuffer headerBuffer = Buffers.allocate(HEADER_BYTES, false);

    public RecordIterator(FileChannel channel, long startOffset, long startPos, AtomicLong writePosition) {
        this.channel = channel;
        this.startOffset = startOffset;
        this.writePosition = writePosition;
        this.lastOffset.set(startOffset - 1);
        this.readPos.set(startPos);
        this.startPos = startPos;
    }

    @Override
    public boolean hasNext() {
        return writePosition.get() > readPos.get() + HEADER_BYTES;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        RecordHeader header = readHeader();
        Record record = Record.readFrom(channel, header, readPos.get());
        readPos.addAndGet(record.size());
        long actualOffset = record.offset;
        long expectedOffset = actualOffset - 1;
        if (!lastOffset.compareAndSet(expectedOffset, record.offset)) {
            throw new IllegalStateException("Non sequential offset read, expected " + expectedOffset + " actual " + actualOffset);
        }
        return record;
    }

    private RecordHeader readHeader() {
        long recordStartPos = readPos.get();
        RecordHeader header;
        do {
            header = RecordHeader.readFrom(channel, headerBuffer, recordStartPos);
            headerBuffer.clear();

            if (header.offset < startOffset) {
                //skip entry
                recordStartPos = readPos.addAndGet(RecordHeader.HEADER_BYTES + header.length);
            }
        } while (header.offset < startOffset);
        return header;
    }

    public long offset() {
        return lastOffset.get();
    }

    public long entriesRead() {
        return (lastOffset.get() - startOffset) + 1;
    }

    public long bytesRead() {
        return readPos.get() - startPos;
    }

    //internal only
    long position() {
        return readPos.get();
    }

}
