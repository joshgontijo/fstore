package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.BufferRef;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.IDataStream;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

class SegmentReader<T> extends TimeoutReader implements SegmentIterator<T> {

    private final Segment segment;
    private final Storage storage;
    private final IDataStream dataStream;
    private final Serializer<T> serializer;
    private final Direction direction;
    private final Queue<T> pageQueue = new ArrayDeque<>(DataStream.MAX_BULK_READ_RESULT);
    private final Queue<Integer> entriesSizes = new ArrayDeque<>(DataStream.MAX_BULK_READ_RESULT);

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
        this.lastReadTs = System.currentTimeMillis();
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
        T entry = pageQueue.poll();
        lastReadTs = System.currentTimeMillis();
        if (entry != null) {
            int recordSize = entriesSizes.poll();
            bytesRead.addAndGet(recordSize);
            entriesRead.incrementAndGet();
            readPosition.updateAndGet(p -> Direction.FORWARD.equals(direction) ? p + recordSize : p - recordSize);
        }
        return entry;
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
        try (BufferRef ref = dataStream.bulkRead(storage, direction, pos)) {
            int[] entriesLength = ref.readAllInto(pageQueue, serializer);
            for (int length : entriesLength) {
                entriesSizes.add(length);
            }
            if (entriesLength.length == 0) {
                emptyReads.incrementAndGet();
            }
        }
    }

    @Override
    public boolean endOfLog() {
        return segment.endOfLog(readPosition.get());
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
                ", lastReadTs=" + lastReadTs +
                '}';
    }
}
