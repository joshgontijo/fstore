package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.header.Type;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

class SegmentReader<T> implements SegmentIterator<T> {

    private final Segment segment;
    private final Storage storage;
    private final IDataStream dataStream;
    private final Serializer<T> serializer;
    private final Direction direction;
    private final Queue<RecordEntry<T>> pageQueue = new ArrayDeque<>(DataStream.MAX_BULK_READ_RESULT);

    private final AtomicLong readPosition = new AtomicLong();
    private final AtomicLong emptyReads = new AtomicLong();
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong entriesRead = new AtomicLong();

    SegmentReader(Segment segment, Storage storage, IDataStream dataStream, Serializer<T> serializer, long initialPosition, Direction direction) {
        this.segment = segment;
        this.storage = storage;
        this.dataStream = dataStream;
        this.direction = direction;
        this.serializer = serializer;
        this.readPosition.set(initialPosition);
    }

    @Override
    public long position() {
        return readPosition.get();
    }

    @Override
    public boolean hasNext() {
        if (pageQueue.isEmpty()) {
            fetchEntries();
        }
        return !pageQueue.isEmpty();
    }

    @Override
    public T next() {
        T poll = getNext();
        if (poll != null) {
            return poll;
        }
        fetchEntries();
        return getNext();
    }

    private T getNext() {
        RecordEntry<T> entry = pageQueue.poll();
        if (entry == null) {
            return null;
        }
        int recordSize = entry.recordSize();
        bytesRead.addAndGet(recordSize);
        entriesRead.incrementAndGet();
        readPosition.updateAndGet(p -> Direction.FORWARD.equals(direction) ? p + recordSize : p - recordSize);
        return entry.entry();
    }

    private void fetchEntries() {
        if (segment.closed()) {
            throw new RuntimeException("Closed segment");
        }
        long pos = readPosition.get();
        if (Direction.FORWARD.equals(direction) && pos >= segment.position()) {
            return;
        }
        if (Direction.BACKWARD.equals(direction) && pos <= Log.START) {
            return;
        }
        List<RecordEntry<T>> entries = dataStream.bulkRead(storage, direction, pos, serializer);
        pageQueue.addAll(entries);
        if (entries.isEmpty()) {
            emptyReads.incrementAndGet();
        }
    }

    @Override
    public boolean endOfLog() {
        return this.position() >= segment.position() && !Type.LOG_HEAD.equals(segment.header.type());
    }

    @Override
    public void close() {
        segment.releaseReader(this);
    }

    @Override
    public String toString() {
        return "SegmentReader{" + "segment=" + segment.name() +
                ", direction=" + direction +
                ", pageQueue=" + pageQueue.size() +
                ", readPosition=" + readPosition +
                ", emptyReads=" + emptyReads +
                ", bytesRead=" + bytesRead +
                ", entriesRead=" + entriesRead +
                '}';
    }
}
