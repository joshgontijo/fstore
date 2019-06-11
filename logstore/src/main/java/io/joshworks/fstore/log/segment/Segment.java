package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Logging;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.log.segment.header.Type;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

/**
 * File format:
 * <p>
 * |---- HEADER ----|----- LOG -----|--- END OF LOG (8bytes) ---|
 * </p>
 * <p>
 * HEADER: 0 -> 1023
 * LOG: 1024 -> fileSize - 8
 * EOL: fileSize - 8 -> fileSize
 * </p>
 * <p>
 * Segment is not thread safe for append method. But it does guarantee concurrent access of multiple readers at same time.
 * Multiple readers can also read (iterator and get) while a record is being appended
 */
public class Segment<T> implements Log<T> {

    private final Logger logger;

    private final Serializer<T> serializer;
    private final Storage storage;
    private final IDataStream dataStream;
    private final String magic;

    protected final AtomicLong entries = new AtomicLong();
    protected final AtomicLong writePosition = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean markedForDeletion = new AtomicBoolean();

    protected final LogHeader header;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Set<TimeoutReader> readers = ConcurrentHashMap.newKeySet();

    //Type is only used for new segments, accepted values are Type.LOG_HEAD or Type.MERGE_OUT
    //Magic is used to create new segment or verify existing
    public Segment(Storage storage, Serializer<T> serializer, IDataStream dataStream, String magic, WriteMode writeMode) {
        try {
            this.serializer = requireNonNull(serializer, "Serializer must be provided");
            this.storage = requireNonNull(storage, "Storage must be provided");
            this.dataStream = requireNonNull(dataStream, "Reader must be provided");
            this.magic = requireNonNull(magic, "Magic must be provided");
            this.logger = Logging.namedLogger(storage.name(), "segment");

            if (storage.length() <= LogHeader.BYTES) {
                throw new IllegalArgumentException("Segment size must greater than " + LogHeader.BYTES);
            }

            this.header = LogHeader.read(storage, magic);
            if (Type.NONE.equals(header.type())) { //new segment
                if (writeMode == null) {
                    throw new SegmentException("Segment doesn't exist, WriteMode must be specified");
                }
                this.header.writeNew(storage, magic, writeMode, storage.length(), false); //TODO update for ENCRYPTION

                this.position(Log.START);
                this.entries.set(0);

            } else { //existing segment
                this.entries.set(header.entries());
                this.position(header.writePosition());
                if (Type.LOG_HEAD.equals(header.type())) {
                    SegmentState result = rebuildState(Segment.START);
                    this.position(result.position);
                    this.entries.set(result.entries);
                }
            }

        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw new SegmentException("Failed to construct segment", e);
        }
    }

    private void position(long position) {
        if (position < START || position >= fileSize()) {
            throw new IllegalArgumentException("Position must be between " + LogHeader.BYTES + " and " + fileSize() + ", got " + position);
        }
        this.storage.writePosition(position);
        this.writePosition.set(position);
    }

    //logical truncation
    public void truncate(long position) {
        position(position);
        int blankSize = (int) Math.min(Memory.PAGE_SIZE, logSize() - position);
        this.storage.write(ByteBuffer.wrap(new byte[blankSize]));
        position(position);
    }

    @Override
    public long append(T data) {
        checkNotClosed();
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        ByteBuffer bytes = serializer.toBytes(data);
        long recordPosition = dataStream.write(storage, bytes);
        if (recordPosition == Storage.EOF) {
            return recordPosition;
        }
        writePosition.set(storage.writePosition());
        incrementEntry();
        return recordPosition;
    }

    protected void incrementEntry() {
        entries.incrementAndGet();
    }

    @Override
    public T get(long position) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            checkBounds(position);
            RecordEntry<T> entry = dataStream.read(storage, Direction.FORWARD, position, serializer);
            return entry == null ? null : entry.entry();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long fileSize() {
        return storage.length();
    }

    @Override
    public long logSize() {
        return fileSize() - LogHeader.BYTES;
    }

    @Override
    public long remaining() {
        return logSize() - position();
    }

    @Override
    public String name() {
        return storage.name();
    }

    @Override
    public SegmentIterator<T> iterator(Direction direction) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            long position = Direction.FORWARD.equals(direction) ? Log.START : position();
            return iterator(position, direction);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SegmentIterator<T> iterator(long position, Direction direction) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            checkNotClosed();
            checkBounds(position);
            SegmentReader<T> segmentReader = new SegmentReader<>(this, storage, dataStream, serializer, position, direction);
            return acquireReader(segmentReader);
        } finally {
            lock.unlock();
        }

    }

    @Override
    public long position() {
        return writePosition.get();
    }

    @Override
    public void close() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            readers.clear(); //evict readers
            IOUtils.closeQuietly(storage);
        } finally {
            lock.unlock();
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
        this.position(storage.length() - 1);
        long position = lastKnownPosition;
        int foundEntries = 0;
        long start = System.currentTimeMillis();
        try {
            logger.info("Restoring log state and checking consistency from position {}", lastKnownPosition);
            int lastRead;
            do {
                List<RecordEntry<T>> entries = dataStream.bulkRead(storage, Direction.FORWARD, position, serializer);
                foundEntries += processEntries(entries);
                lastRead = entries.stream().mapToInt(RecordEntry::recordSize).sum();
                position += lastRead;

            } while (lastRead > 0);

        } catch (Exception e) {
            logger.warn("Found inconsistent entry on position " + position + ", segment '" + name() + "': " + e.getMessage());
            storage.writePosition(position);
            dataStream.write(storage, ByteBuffer.wrap(EOL));
            storage.writePosition(position);
        }
        logger.info("Log state restored in {}ms, current position: {}, entries: {}", (System.currentTimeMillis() - start), position, foundEntries);
        if (position < Log.START) {
            throw new IllegalStateException("Initial log state position must be at least " + Log.START);
        }
        return new SegmentState(foundEntries, position);
    }

    protected long processEntries(List<RecordEntry<T>> items) {
        return items.size();
    }

    @Override
    public void delete() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            if (!markedForDeletion.compareAndSet(false, true)) {
                return;
            }
            this.header.writeDeleted(storage);
            if (readers.isEmpty()) {
                deleteInternal();
            } else {
                logger.info("{} active readers while deleting, marked for deletion", readers.size());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void roll(int level) {
        if (readOnly()) {
            throw new IllegalStateException("Cannot roll read only segment: " + this.toString());
        }

        long currPos = storage.writePosition();
        long uncompressedSize = uncompressedSize();
        this.header.writeCompleted(storage, entries.get(), level, currPos, uncompressedSize);
    }

    @Override
    public boolean readOnly() {
        return Type.READ_ONLY.equals(header.type());
    }

    @Override
    public boolean closed() {
        return closed.get();
    }

    @Override
    public long entries() {
        return entries.get();
    }

    @Override
    public int level() {
        return header.level();
    }

    @Override
    public long created() {
        return header.created();
    }

    @Override
    public long uncompressedSize() {
        return logSize();
    }

    @Override
    public Type type() {
        return this.header.type();
    }

    //safely check if the given position is at the end of the log
    boolean endOfLog(long position) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            return readOnly() && position >= position();
        } finally {
            lock.unlock();
        }
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new SegmentClosedException("Segment " + name() + "is closed");
        }
    }

    private void checkBounds(long position) {
        if (position < START) {
            throw new IllegalArgumentException("Position must be greater or equals to " + START + ", got: " + position);
        }
        long writePosition = position();
        if (position > writePosition) {
            throw new IllegalArgumentException("Position (" + position + ") must be less than writePosition " + writePosition);
        }
    }

    private <R extends TimeoutReader> R acquireReader(R reader) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            if (closed.get()) {
                throw new RuntimeException("Could not acquire segment reader: Closed");
            }
            if (markedForDeletion.get()) {
                throw new RuntimeException("Could not acquire segment reader: Marked for deletion");
            }
            readers.add(reader);
            return reader;
        } finally {
            lock.unlock();
        }
    }

    <R extends TimeoutReader> void releaseReader(R reader) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            boolean removed = readers.remove(reader);
            if (removed) { //may be called multiple times for the same reader
                if (markedForDeletion.get() && readers.isEmpty()) {
                    deleteInternal();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void deleteInternal() {
        String name = name();
        logger.info("Deleting {}", name);
        storage.delete();
        close();
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
}
