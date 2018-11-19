package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Logging;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.BufferRef;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.log.segment.header.Type;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

/**
 * File format:
 * <p>
 * |---- HEADER ----|----- LOG -----|--- END OF LOG (8bytes) ---|
 */
public class Segment<T> implements Log<T> {

    private final Logger logger;

    private final Serializer<T> serializer;
    private final Storage storage;
    private final IDataStream dataStream;
    private final String magic;

    protected AtomicLong entries = new AtomicLong();
    final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean markedForDeletion = new AtomicBoolean();

    private LogHeader header;

    private final Object READER_LOCK = new Object();
    private final Set<TimeoutReader> readers = ConcurrentHashMap.newKeySet();

    //Type is only used for new segments, accepted values are Type.LOG_HEAD or Type.MERGE_OUT
    //Magic is used to create new segment or verify existing
    public Segment(Storage storage, Serializer<T> serializer, IDataStream dataStream, String magic, Type type) {
        try {
            this.serializer = requireNonNull(serializer, "Serializer must be provided");
            this.storage = requireNonNull(storage, "Storage must be provided");
            this.dataStream = requireNonNull(dataStream, "Reader must be provided");
            this.magic = requireNonNull(magic, "Magic must be provided");
            this.logger = Logging.namedLogger(storage.name(), "segment");

            LogHeader foundHeader = LogHeader.read(storage);
            if (foundHeader == null) {
                if (type == null) {
                    throw new SegmentException("Segment doesn't exist, " + Type.LOG_HEAD + " or " + Type.MERGE_OUT + " must be specified");
                }
                this.header = LogHeader.writeNew(storage, magic, type, storage.length());

                this.position(Log.START);
                this.entries.set(this.header.entries);

            } else {
                this.header = foundHeader;
                this.entries.set(foundHeader.entries);
                if (Type.LOG_HEAD.equals(foundHeader.type)) {
                    SegmentState result = rebuildState(Segment.START);
                    this.position(result.position);
                    this.entries.set(result.entries);
                }

            }
            LogHeader.validateMagic(this.header.magic, magic);

        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw new SegmentException("Failed to construct segment", e);
        }
    }

    private void position(long position) {
        if (position < START) {
            throw new IllegalArgumentException("Position must be at least " + LogHeader.BYTES);
        }
        this.storage.position(position);
    }

    @Override
    public long append(T data) {
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        ByteBuffer bytes = serializer.toBytes(data);
        long recordPosition = dataStream.write(storage, bytes);
        incrementEntry();
        return recordPosition;
    }

    protected void incrementEntry() {
        entries.incrementAndGet();
    }

    @Override
    public T get(long position) {
        checkBounds(position);
        try (BufferRef ref = dataStream.read(storage, Direction.FORWARD, position)) {
            ByteBuffer bb = ref.get();
            if (bb.remaining() == 0) { //EOF
                return null;
            }
            return serializer.fromBytes(bb);
        }
    }

    private void checkBounds(long position) {
        if (position < START) {
            throw new IllegalArgumentException("Position must be greater or equals to " + START + ", got: " + position);
        }
        long logicalSize = logicalSize();
        if (position > logicalSize) {
            throw new IllegalArgumentException("Position must be less than logicalSize " + logicalSize + ", got " + position);
        }
    }

    @Override
    public long fileSize() {
        return storage.length();
    }

    @Override
    public long logicalSize() {
        return readOnly() ? header.logicalSize : position();
    }

    @Override
    public Set<TimeoutReader> readers() {
        return readers;
    }

    @Override
    public String name() {
        return storage.name();
    }

    @Override
    public SegmentIterator<T> iterator(Direction direction) {
        if (Direction.FORWARD.equals(direction)) {
            return iterator(START, direction);
        }
        long startPos = readOnly() ? header.logicalSize : position();
        return iterator(startPos, direction);

    }

    @Override
    public SegmentIterator<T> iterator(long position, Direction direction) {
        checkClosed();
        return newLogReader(position, direction);
    }

    @Override
    public long position() {
        return storage.position();
    }

    @Override
    public void close() {
        synchronized (READER_LOCK) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            readers.clear(); //evict readers
            IOUtils.closeQuietly(storage);
        }
    }

    @Override
    public void flush() {
        try {
            storage.flush();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public SegmentState rebuildState(long lastKnownPosition) {
        if (lastKnownPosition < START) {
            throw new IllegalStateException("Invalid lastKnownPosition: " + lastKnownPosition + ",value must be at least " + START);
        }
        long position = lastKnownPosition;
        int foundEntries = 0;
        long start = System.currentTimeMillis();
        try {
            logger.info("Restoring log state and checking consistency from position {}", lastKnownPosition);
            int lastRead;
            do {
                try (BufferRef ref = dataStream.bulkRead(storage, Direction.FORWARD, position)) {
                    List<T> items = new ArrayList<>();
                    int[] recordSizes = ref.readAllInto(items, serializer);
                    if (!items.isEmpty()) {
                        foundEntries += processEntries(items);
                    }
                    lastRead = IntStream.of(recordSizes).sum();
                    position += lastRead;
                }
            } while (lastRead > 0);

        } catch (Exception e) {
            logger.warn("Found inconsistent entry on position {}, segment '{}': {}", position, name(), e.getMessage());
            storage.position(position);
            dataStream.write(storage, ByteBuffer.wrap(EOL));
            storage.position(position);
        }
        logger.info("Log state restored in {}ms, current position: {}, entries: {}", (System.currentTimeMillis() - start), position, foundEntries);
        if (position < Log.START) {
            throw new IllegalStateException("Initial log state position must be at least " + Log.START);
        }
        return new SegmentState(foundEntries, position);
    }

    protected long processEntries(List<T> items) {
        return items.size();
    }

    @Override
    public void delete() {
        synchronized (READER_LOCK) {
            if (!markedForDeletion.compareAndSet(false, true)) {
                return;
            }
            LogHeader.writeDeleted(storage, this.header);
            if (readers.isEmpty()) {
                deleteInternal();
            } else {
                logger.info("{} active readers while deleting, marked for deletion", readers.size());
            }

        }
    }

    private void deleteInternal() {
        String name = name();
        logger.info("Deleting {}", name);
        storage.delete();
        close();
        logger.info("Deleted {}", name);
    }

    @Override
    public void roll(int level) {
        if (readOnly()) {
            throw new IllegalStateException("Cannot roll read only segment: " + this.toString());
        }

        long currPos = storage.position();
        writeEndOfLog();
        this.header = LogHeader.writeCompleted(storage, this.header, entries.get(), level, currPos);

    }

    private <R extends TimeoutReader> R acquireReader(R reader) {
        synchronized (READER_LOCK) {
            if (closed.get()) {
                throw new RuntimeException("Could not acquire segment reader: Closed");
            }
            if (markedForDeletion.get()) {
                throw new RuntimeException("Could not acquire segment reader: Marked for deletion");
            }
            readers.add(reader);
            return reader;
        }
    }

    private <R extends TimeoutReader> void removeFromReaders(R reader) {
        synchronized (READER_LOCK) {
            boolean removed = readers.remove(reader);
            if (removed) { //may be called multiple times for the same reader
                if (markedForDeletion.get() && readers.isEmpty()) {
                    deleteInternal();
                }
            }
        }
    }

    private void writeEndOfLog() {
        storage.write(ByteBuffer.wrap(Log.EOL));
    }

    //TODO implement race condition on acquiring readers and closing / deleting segment
    //ideally the delete functionality would be moved to inside the segment, instead handling in the Compactor
    private SegmentReader newLogReader(long pos, Direction direction) {
        checkClosed();
        checkBounds(pos);
        SegmentReader segmentReader = new SegmentReader(this, dataStream, serializer, pos, direction);
        return acquireReader(segmentReader);
    }

    @Override
    public boolean readOnly() {
        return Type.READ_ONLY.equals(header.type);
    }

    @Override
    public long entries() {
        return entries.get();
    }

    @Override
    public int level() {
        return header.level;
    }

    @Override
    public long created() {
        return header.created;
    }

    @Override
    public Type type() {
        return header.type;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Segment<?> segment = (Segment<?>) o;
        return Objects.equals(storage, segment.storage) &&
                Objects.equals(magic, segment.magic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storage, magic);
    }

    @Override
    public String toString() {
        return "LogSegment{" + "handler=" + storage.name() +
                ", entries=" + entries() +
                ", header=" + header +
                ", readers=" + Arrays.toString(readers.toArray()) +
                '}';
    }

    private void checkClosed() {
        if (closed.get()) {
            throw new SegmentClosedException("Segment " + name() + "is closed");
        }
    }

    private class SegmentReader extends TimeoutReader implements SegmentIterator<T> {

        private Segment segment;
        private final IDataStream dataStream;
        private final Serializer<T> serializer;
        private final Direction direction;
        private final Queue<T> pageQueue = new ArrayDeque<>(DataStream.MAX_BULK_READ_RESULT);
        private final Queue<Integer> entriesSizes = new ArrayDeque<>(DataStream.MAX_BULK_READ_RESULT);

        protected long position;

        SegmentReader(Segment segment, IDataStream dataStream, Serializer<T> serializer, long initialPosition, Direction direction) {
            this.segment = segment;
            this.dataStream = dataStream;
            this.direction = direction;
            this.serializer = serializer;
            this.position = initialPosition;
            this.lastReadTs = System.currentTimeMillis();
        }

        @Override
        public long position() {
            return position;
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
                position = Direction.FORWARD.equals(direction) ? position + recordSize : position - recordSize;
            }
            return entry;
        }

        private void fetchEntries() {
            if (segment.closed.get()) {
                throw new RuntimeException("Closed segment");
            }
            if (Direction.FORWARD.equals(direction) && position >= segment.logicalSize()) {
                return;
            }
            if (Direction.BACKWARD.equals(direction) && position <= Log.START) {
                return;
            }
            try (BufferRef ref = dataStream.bulkRead(segment.storage, direction, position)) {
                int[] entriesLength = ref.readAllInto(pageQueue, serializer);
                for (int length : entriesLength) {
                    entriesSizes.add(length);
                }
                if(entriesLength.length == 0) {
                    logger.warn("Empty read");
                }
            }
        }

        @Override
        public void close() {
            segment.removeFromReaders(this);
        }

        @Override
        public String toString() {
            return "SegmentReader{ position=" + position +
                    ", order=" + direction +
                    ", lastReadTs=" + lastReadTs +
                    '}';
        }

        @Override
        public boolean endOfLog() {
            return Segment.this.readOnly() && position >= Segment.this.logicalSize();
        }
    }

}
