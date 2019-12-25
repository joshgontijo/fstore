package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.MetricStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.core.util.Logging;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.footer.FooterMap;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;
import org.slf4j.Logger;

import java.io.File;
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
import java.util.function.Consumer;

import static io.joshworks.fstore.core.io.Storage.EOF;
import static java.util.Objects.requireNonNull;

/**
 * File format:
 * |---- HEADER ----|----- LOG -----|--- EOL ---| --- FOOTER ---|
 * Segment is not thread safe for append method. But it does guarantee concurrent access of multiple readers at same time.
 * Multiple readers can also read (iterator and get) while a record is being appended
 */
public class Segment<T> implements Log<T> {

    private final Logger logger;

    private static final double FOOTER_EXTRA_LENGTH_PERCENT = 0.1;

    private final Serializer<T> serializer;
    private final BufferPool bufferPool;
    private final MetricStorage storage;
    private final DataStream stream;

    protected final AtomicLong entries = new AtomicLong();
    //writePosition must be kept separate from store, so it allows reads happening when rolling segment
    private final AtomicLong dataWritePosition = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean markedForDeletion = new AtomicBoolean();

    private final FooterMap footerMap = new FooterMap();

    final LogHeader header;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Set<SegmentIterator<T>> readers = ConcurrentHashMap.newKeySet();

    private final Metrics metrics = new Metrics();

    //Type is only used for new segments, accepted values are Type.LOG_HEAD or Type.MERGE_OUT
    public Segment(
            File file,
            StorageMode storageMode,
            long segmentDataSize,
            Serializer<T> serializer,
            BufferPool bufferPool,
            WriteMode writeMode,
            double checksumProb) {

        this.serializer = requireNonNull(serializer, "Serializer must be provided");
        this.bufferPool = bufferPool;

        MetricStorage storage = null;
        try {
            long alignedSegmentDataSize = Storage.align(segmentDataSize);
            long fileLength = getTotalFileLength(alignedSegmentDataSize, bufferPool.bufferSize());
            this.storage = storage = new MetricStorage(Storage.createOrOpen(file, storageMode, fileLength));
            this.stream = new DataStream(bufferPool, storage, checksumProb, Memory.PAGE_SIZE);
            this.logger = Logging.namedLogger(storage.name(), "segment");

            if (storage.length() <= LogHeader.BYTES) {
                throw new IllegalArgumentException("Segment size must greater than " + LogHeader.BYTES);
            }
            this.header = LogHeader.read(stream);
            if (Type.NONE.equals(header.type())) { //new segment
                if (writeMode == null) {
                    throw new SegmentException("Segment doesn't exist, WriteMode must be specified");
                }
                this.header.writeNew(writeMode, fileLength, alignedSegmentDataSize, false); //TODO update for ENCRYPTION

                this.setPosition(Log.START);
                this.entries.set(0);

            } else { //existing segment
                this.entries.set(header.entries());
                this.setPosition(header.logicalSize());
                if (Type.LOG_HEAD.equals(header.type())) {
                    SegmentState result = rebuildState();
                    this.setPosition(result.position);
                    this.entries.set(result.entries);
                }
            }
            footerMap.load(header, stream);

        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw new SegmentException("Failed to construct segment", e);
        }
    }

    @Override
    public long append(T record) {
        checkClosed();
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        if (remaining() <= 0) {
            return EOF;
        }
        long recordPosition = stream.write(record, serializer);
        incrementEntry();
        dataWritePosition.set(storage.position());
        metrics.update("append");
        return recordPosition;
    }

    public FooterReader footerReader() {
        return new FooterReader(stream, footerMap);
    }

    @Override
    public T get(long position) {
        checkClosed();
        checkBounds(position);
        RecordEntry<T> entry = stream.read(Direction.FORWARD, position, serializer);
        metrics.update("get");
        return entry.entry();
    }

    public void read(long position, ByteBuffer buffer) {
        checkClosed();
        checkBounds(position);
        stream.read(Direction.FORWARD, position, Serializers.transfer(buffer));
    }

    @Override
    public long remaining() {
        if (header.readOnly()) {
            return 0;
        }
        return Math.max(0, header.dataSize() - dataWritten());
    }

    @Override
    public long physicalSize() {
        return header.physicalSize();
    }

    @Override
    public long logicalSize() {
        return header.logicalSize();
    }

    @Override
    public long dataSize() {
        return header.dataSize();
    }

    @Override
    public long actualDataSize() {
        return header.actualDataSize();
    }

    @Override
    public long uncompressedDataSize() {
        return header.uncompressedDataSize();
    }

    @Override
    public long headerSize() {
        return header.headerSize();
    }

    @Override
    public long footerSize() {
        return header.footerSize();
    }

    public long dataWritten() {
        return position() - START;
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
            checkClosed();
            checkBounds(position);
            SegmentReader<T> segmentReader = new SegmentReader<>(this, stream, serializer, position, direction);
            return acquireReader(segmentReader);
        } finally {
            lock.unlock();
        }

    }

    @Override
    public long position() {
        return dataWritePosition.get();
    }

    @Override
    public void close() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            for (SegmentIterator reader : readers) {
                releaseReader(reader);
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
    public void delete() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            if (!markedForDeletion.compareAndSet(false, true)) {
                return;
            }
            this.header.writeDeleted();
            if (readers.isEmpty()) {
                deleteInternal();
            } else {
                logger.info("{} active readers while deleting, marked for deletion", readers.size());
            }
        } finally {
            lock.unlock();
        }
    }

    public void roll(int level, boolean trim, Consumer<FooterWriter> footerWriter) {
        if (readOnly()) {
            throw new IllegalStateException("Cannot roll read only segment: " + this.toString());
        }

        long maxDataPosition = storage.position();
        long actualDataSize = maxDataPosition - START;
        long uncompressedSize = uncompressedSize();

        storage.write(ByteBuffer.wrap(EOL)); //do not use stream

        FooterWriter footer = new FooterWriter(stream, footerMap);
        long footerStart = stream.position();
        footerWriter.accept(footer);

        long mapPosition = footerMap.writeTo(stream, bufferPool);
        long footerEnd = stream.position();
        long footerLength = footerEnd - footerStart;
        if (trim) {
            storage.truncate(footerEnd);
        }
        this.header.writeCompleted(entries.get(), level, actualDataSize, mapPosition, footerStart, footerLength, uncompressedSize, storage.length());
    }

    @Override
    public void roll(int level, boolean trim) {
        roll(level, trim, fw -> {});
    }

    @Override
    public boolean readOnly() {
        return header.readOnly();
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
        return actualDataSize();
    }

    @Override
    public Type type() {
        return this.header.type();
    }

    <R extends SegmentIterator<T>> void releaseReader(R reader) {
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

    private void incrementEntry() {
        entries.incrementAndGet();
    }

    private SegmentState rebuildState() {
        this.setPosition(header.maxDataPosition());
        long position = START;
        int foundEntries = 0;
        long start = System.currentTimeMillis();
        try {
            logger.info("Restoring log state and checking consistency");
            int lastRead;
            do {
                List<RecordEntry<T>> entries = stream.bulkRead(Direction.FORWARD, position, serializer);
                foundEntries += entries.size();
                lastRead = entries.stream().mapToInt(RecordEntry::recordSize).sum();
                position += lastRead;

            } while (lastRead > 0);

        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Found inconsistent entry on position " + position + ", segment '" + name() + "': " + e.getMessage());
            storage.write(position, ByteBuffer.wrap(EOL));
            storage.position(position);
        }
        logger.info("Log state restored in {}ms, current position: {}, entries: {}", (System.currentTimeMillis() - start), position, foundEntries);
        if (position < Log.START) {
            throw new IllegalStateException("Initial log state position must be at least " + Log.START);
        }
        return new SegmentState(foundEntries, position);
    }

    private void checkClosed() {
        if (closed.get()) {
            throw new SegmentException("Segment " + name() + " is closed");
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

    private <R extends SegmentIterator<T>> R acquireReader(R reader) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            if (closed.get()) {
                throw new SegmentException("Segment '" + name() + "' is closed");
            }
            if (markedForDeletion.get()) {
                throw new SegmentException("Segment '" + name() + "' is marked for deletion");
            }
            if (!readers.add(reader)) {
                throw new IllegalStateException("Failed to acquire reader: Already exist in the readers list");
            }
            return reader;
        } finally {
            lock.unlock();
        }
    }

    private long getTotalFileLength(long segmentDataSize, int maxEntrySize) {
        //TODO ideally footer would have its own size, so it can
        long footerExtra = Storage.align((long) (FOOTER_EXTRA_LENGTH_PERCENT * segmentDataSize));
        //Adding maxEntrySize makes sure storage will not need to resize when writing data
        //This makes data storage more predictable and slightly faster since, the file will not need resizing in some cases
        long withAdditionAlEntry = segmentDataSize + Storage.align(maxEntrySize);
        return Storage.align(LogHeader.BYTES + withAdditionAlEntry + EOL.length + footerExtra);
    }

    //can only be used for data
    private void setPosition(long position) {
        if (position < START) {
            throw new IllegalArgumentException("Position must greater or equals than " + LogHeader.BYTES + ", got " + position);
        }
        long maxDataPos = header.maxDataPosition();

        long maxWritePos = Math.min(position, maxDataPos);
        this.dataWritePosition.set(maxWritePos);
        this.storage.position(position);
    }

    private void deleteInternal() {
        logger.info("Deleting {}", name());
        storage.delete();
        close();
    }

    @Override
    public Metrics metrics() {
        metrics.set("physicalSize", physicalSize());
        metrics.set("logicalSize", logicalSize());
        metrics.set("dataSize", dataSize());
        metrics.set("actualDataSize", actualDataSize());
        metrics.set("entries", entries());
        metrics.set("headerSize", headerSize());
        metrics.set("footerSize", footerSize());
        metrics.set("uncompressedDataSize", uncompressedDataSize());
        return Metrics.merge(storage.metrics(), metrics);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Segment<?> segment = (Segment<?>) o;
        return Objects.equals(storage, segment.storage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storage);
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
