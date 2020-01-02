package io.joshworks.ilog;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.joshworks.ilog.RecordHeader.HEADER_BYTES;

public class RecordIterator implements Iterator<Record> {

    protected final Storage storage;
    protected final long startOffset;
    protected final Supplier<Long> writePosition;
    protected final long startPos;
    protected final AtomicLong readPos = new AtomicLong();
    protected final AtomicLong lastOffset = new AtomicLong();

    private final ByteBuffer headerBuffer = Buffers.allocate(HEADER_BYTES, false);

    public RecordIterator(Storage storage, long startOffset, long startPos, Supplier<Long> writePosition) {
        this.storage = storage;
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
        Record record = Record.readFrom(storage, header, readPos.get());
        readPos.addAndGet(record.size());

        return checkAndUpdateOffset(record);
    }

    protected Record checkAndUpdateOffset(Record record) {
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
            header = RecordHeader.readFrom(storage, headerBuffer, recordStartPos);
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
