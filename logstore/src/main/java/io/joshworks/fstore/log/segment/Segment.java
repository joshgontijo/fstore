package io.joshworks.fstore.log.segment;


import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.AdaptiveBufferPool;
import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.record.BufferRef;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.record.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * File format:
 * <p>
 * |---- HEADER ----|----- LOG -----|--- END OF LOG (8bytes) ---|---- FOOTER ----|
 */
public class Segment<T> implements Log<T> {

    private static final Logger logger = LoggerFactory.getLogger(Segment.class);

    private final Serializer<LogHeader> headerSerializer = new HeaderSerializer();
    private final Serializer<T> serializer;
    private final Storage storage;
    private final IDataStream dataStream;
    private final String magic;
    private final BufferPool bufferPool = new AdaptiveBufferPool(1000, false);

    private AtomicLong entries = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    private LogHeader header;

    private final Set<TimeoutReader> readers = ConcurrentHashMap.newKeySet();

    //Type is only used for new segments, accepted values are Type.LOG_HEAD or Type.MERGE_OUT
    //Magic is used to create new segment or verify existing
    public Segment(Storage storage, Serializer<T> serializer, IDataStream dataStream, String magic, Type type) {
        this.serializer = requireNonNull(serializer, "Serializer must be provided");
        this.storage = requireNonNull(storage, "Storage must be provided");
        this.dataStream = requireNonNull(dataStream, "Reader must be provided");
        this.magic = requireNonNull(magic, "Magic must be provided");

        LogHeader readHeader = readHeader(storage);

        if (LogHeader.noHeader().equals(readHeader)) { //new segment
            if (Type.OPEN.equals(type)) {
                IOUtils.closeQuietly(storage);
                throw new SegmentException("Segment doesn't exist, " + Type.LOG_HEAD + " or " + Type.MERGE_OUT + " must be specified");
            }
            this.header = createNewHeader(storage, type, magic);
            this.position(START);
            return;
        }
        this.header = readHeader;

        byte[] expected = header.magic.getBytes(StandardCharsets.UTF_8);
        byte[] actual = magic.getBytes(StandardCharsets.UTF_8);

        if (!Arrays.equals(expected, actual)) {
            IOUtils.closeQuietly(storage);
            throw new InvalidMagic(header.magic, magic);
        }
        this.position(LogHeader.BYTES);

        entries.set(header.entries);
        if (Type.LOG_HEAD.equals(header.type)) { //reopening log head
            SegmentState result = rebuildState(Log.START);
            this.position(result.position);
            entries.set(result.entries);
        }
    }

    private LogHeader readHeader(Storage storage) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.BYTES);
        storage.read(0, bb);
        bb.flip();
        if (bb.remaining() == 0) {
            return LogHeader.noHeader();
        }
        return headerSerializer.fromBytes(bb);

    }

    private LogHeader createNewHeader(Storage storage, Type type, String magic) {
        validateTypeProvided(type);
        LogHeader newHeader = LogHeader.create(magic, type);
        ByteBuffer headerData = headerSerializer.toBytes(newHeader);
        if (storage.write(headerData) != LogHeader.BYTES) {
            throw new SegmentException("Failed to create header");
        }
        try {
            storage.flush();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
        return newHeader;
    }

    private void validateTypeProvided(Type type) {
        //new segment, create a header
        if (type == null) {
            throw new IllegalArgumentException("Type must provided when creating a new segment");
        }
        if (!Type.LOG_HEAD.equals(type) && !Type.MERGE_OUT.equals(type)) {
            throw new IllegalArgumentException("Only Type.LOG_HEAD and Type.MERGE_OUT are accepted when creating a segment");
        }
    }

    private void position(long position) {
        if (position < START) {
            throw new IllegalArgumentException("Position must be at least " + LogHeader.BYTES);
        }
        this.storage.position(position);
    }

    @Override
    public long position() {
        return storage.position();
    }

    @Override
    public Marker marker() {
        if (readOnly()) {
            return new Marker(header.logStart, header.logEnd, header.footerStart, header.footerEnd);
        }
        return new Marker(header.logStart, -1, -1, -1);
    }

    @Override
    public T get(long position) {
        checkBounds(position);
        try (BufferRef ref = dataStream.read(storage, bufferPool, Direction.FORWARD, position)) {
            ByteBuffer bb = ref.get();
            if (bb.remaining() == 0) { //EOF
                return null;
            }
            return serializer.fromBytes(bb);
        }
    }

    @Override
    public PollingSubscriber<T> poller(long position) {
        checkClosed();
        SegmentPoller segmentPoller = new SegmentPoller(dataStream, serializer, position);
        return addToReaders(segmentPoller);
    }

    @Override
    public PollingSubscriber<T> poller() {
        return poller(Log.START);
    }

    private void checkBounds(long position) {
        if (position < START) {
            throw new IllegalArgumentException("Position must be greater or equals to " + START + ", got: " + position);
        }
        if (readOnly() && position > header.logEnd) {
            throw new IllegalArgumentException("Position must be less than " + header.logEnd + ", got " + position);
        }
    }

    @Override
    public long size() {
        return storage.position();
    }

    @Override
    public Set<TimeoutReader> readers() {
        return readers;
    }

    @Override
    public long append(T data) {
        ByteBuffer bytes = serializer.toBytes(data);
        long recordPosition = dataStream.write(storage, bufferPool, bytes);
        entries.incrementAndGet();
        return recordPosition;
    }

    @Override
    public String name() {
        return storage.name();
    }

    @Override
    public Stream<T> stream(Direction direction) {
        return Iterators.closeableStream(iterator(direction));
    }

    @Override
    public LogIterator<T> iterator(Direction direction) {
        if (Direction.FORWARD.equals(direction)) {
            return iterator(START, direction);
        }
        if (readOnly()) {
            return iterator(header.logEnd, direction);
        }
        return iterator(position(), direction);

    }

    @Override
    public LogIterator<T> iterator(long position, Direction direction) {
        checkClosed();
        return newLogReader(position, direction);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        readers.clear();
        IOUtils.closeQuietly(storage);
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
                try (BufferRef ref = dataStream.read(storage, bufferPool, Direction.FORWARD, position)) {
                    int entrySize = ref.get().remaining();
                    lastRead = entrySize;
                    if (entrySize > 0) {
                        position += entrySize + RecordHeader.HEADER_OVERHEAD;
                        foundEntries++;
                    }
                }
            } while (lastRead > 0);

        } catch (Exception e) {
            logger.warn("Found inconsistent entry on position {}, segment '{}': {}", position, name(), e.getMessage());
        }
        logger.info("Log state restored in {}ms, current position: {}, entries: {}", (System.currentTimeMillis() - start), position, foundEntries);
        if (position < LogHeader.BYTES) {
            throw new IllegalStateException("Initial log state position must be at least " + LogHeader.BYTES);
        }
        return new SegmentState(foundEntries, position);
    }

    @Override
    public void delete() {
        close();
        storage.delete();
    }

    @Override
    public void roll(int level) {
        roll(level, null);
    }

    @Override
    public void roll(int level, ByteBuffer footer) {
        if (Type.READ_ONLY.equals(header.type)) {
            throw new IllegalStateException("Cannot roll read only segment");
        }

        writeEndOfLog();
        long endOfLog = storage.position();
        FooterInfo footerInfo = footer != null ? writeFooter(footer) : FooterInfo.emptyFooter(endOfLog);
        this.header = writeHeader(level, footerInfo);

        boolean hasFooter = header.footerStart > 0;
        long endOfSegment = hasFooter ? header.footerStart + header.footerEnd : endOfLog;
        storage.truncate(endOfSegment);
        storage.markAsReadOnly();
    }

    @Override
    public ByteBuffer readFooter() {
        if (!readOnly()) {
            throw new IllegalStateException("Segment is not read only");
        }
        if (header.footerStart <= 0 || header.footerEnd <= 0) {
            return ByteBuffer.allocate(0);
        }

        ByteBuffer footer = ByteBuffer.allocate((int) (header.footerEnd - header.footerStart));
        storage.read(header.footerStart, footer);
        footer.flip();
        return footer;
    }

    private <R extends TimeoutReader> R addToReaders(R reader) {
        readers.add(reader);
        return reader;
    }

    private <R extends TimeoutReader> void removeFromReaders(R reader) {
        readers.remove(reader);
    }

    private void writeEndOfLog() {
        storage.write(ByteBuffer.wrap(Log.EOL));
    }

    private FooterInfo writeFooter(ByteBuffer footer) {
        long pos = storage.position();
        int size = footer.remaining();
        storage.write(footer);
        return new FooterInfo(pos, pos + size);
    }

    private LogHeader writeHeader(int level, FooterInfo footerInfo) {
        long segmentSize = footerInfo.end;
        long logEnd = footerInfo.start - EOL.length;
        LogHeader newHeader = LogHeader.create(this.magic, entries.get(), this.header.created, level, Type.READ_ONLY, segmentSize, START, logEnd, footerInfo.start, footerInfo.end);
        storage.position(0);
        ByteBuffer headerData = headerSerializer.toBytes(newHeader);
        storage.write(headerData);
        return newHeader;
    }

    //TODO implement race condition on acquiring readers and closing / deleting segment
    //TODO readers limit ?
    private SegmentReader newLogReader(long pos, Direction direction) {

        while (readers.size() >= 10) {
            try {
                Thread.sleep(1000);
                logger.info("Waiting to acquire reader");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        SegmentReader segmentReader = new SegmentReader(dataStream, serializer, pos, direction);
        return addToReaders(segmentReader);
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Segment<?> that = (Segment<?>) o;
        return entries.equals(that.entries) &&
                Objects.equals(headerSerializer, that.headerSerializer) &&
                Objects.equals(serializer, that.serializer) &&
                Objects.equals(storage, that.storage) &&
                Objects.equals(dataStream, that.dataStream) &&
                Objects.equals(header, that.header);
    }

    @Override
    public int hashCode() {

        return Objects.hash(headerSerializer, serializer, storage, dataStream, entries, header);
    }

    @Override
    public String toString() {
        return "LogSegment{" + "handler=" + storage.name() +
                ", entries=" + entries +
                ", header=" + header +
                ", readers=" + Arrays.toString(readers.toArray()) +
                '}';
    }

    private void checkClosed() {
        if (closed.get()) {
            throw new IllegalStateException("Segment " + name() + "is closed");
        }
    }

    private static class FooterInfo {

        private final long start;
        private final long end;

        private FooterInfo(long start, long end) {
            this.start = start;
            this.end = end;
        }

        private static FooterInfo emptyFooter(long start) {
            return new FooterInfo(start, start);
        }

    }

    //NOT THREAD SAFE
    private class SegmentReader extends TimeoutReader implements LogIterator<T> {

        private final IDataStream dataStream;
        private final Serializer<T> serializer;

        protected long position;
        private long readAheadPosition;
        private final Direction direction;
        private final Queue<T> pageQueue = new LinkedList<>();
        private final Queue<Integer> entriesSizes = new LinkedList<>();

        SegmentReader(IDataStream dataStream, Serializer<T> serializer, long initialPosition, Direction direction) {
            this.dataStream = dataStream;
            this.direction = direction;
            checkBounds(initialPosition);
            this.serializer = serializer;
            this.position = initialPosition;
            this.readAheadPosition = initialPosition;
            readAhead();
            this.lastReadTs = System.currentTimeMillis();
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public boolean hasNext() {
            return !pageQueue.isEmpty();
        }

        @Override
        public T next() {
            if (pageQueue.isEmpty()) {
                close();
                throw new NoSuchElementException();
            }
            lastReadTs = System.currentTimeMillis();

            T current = pageQueue.poll();
            int recordSize = entriesSizes.poll();
            if (pageQueue.isEmpty()) {
                readAhead();
            }

            position = Direction.FORWARD.equals(direction) ? position + recordSize : position - recordSize;
            return current;
        }

        private void readAhead() {
            if (Direction.FORWARD.equals(direction)) {
                if (Segment.this.readOnly() && readAheadPosition >= Segment.this.header.logEnd) {
                    return;
                }
                if (!Segment.this.readOnly() && readAheadPosition >= Segment.this.position()) {
                    return;
                }
            }
            if (Direction.BACKWARD.equals(direction) && readAheadPosition <= START) {
                return;
            }
            int totalRead = 0;
            try (BufferRef ref = dataStream.bulkRead(storage, bufferPool, direction, readAheadPosition)) {
                int[] entriesLength = ref.readAllInto(pageQueue, serializer);
                for (int length : entriesLength) {
                    entriesSizes.add(length);
                    totalRead += length;
                }

                if (entriesLength.length == 0) {
                    close();
                    return;
                }
                readAheadPosition = Direction.FORWARD.equals(direction) ? readAheadPosition + totalRead : readAheadPosition - totalRead;
            }

        }

        @Override
        public void close() {
            Segment.this.removeFromReaders(this);
        }

        @Override
        public String toString() {
            return "SegmentReader{ readPosition=" + readAheadPosition +
                    ", order=" + direction +
                    ", readAheadPosition=" + readAheadPosition +
                    ", lastReadTs=" + lastReadTs +
                    '}';
        }
    }

    private class SegmentPoller extends TimeoutReader implements PollingSubscriber<T> {

        private static final int VERIFICATION_INTERVAL_MILLIS = 500;

        private final IDataStream dataStream;
        private final Serializer<T> serializer;
        private final Queue<T> pageQueue = new LinkedList<>();
        private final Queue<Integer> entriesSizes = new LinkedList<>();
        private long readPosition;

        SegmentPoller(IDataStream dataStream, Serializer<T> serializer, long initialPosition) {
            checkBounds(initialPosition);
            this.dataStream = dataStream;
            this.serializer = serializer;
            this.readPosition = initialPosition;
            this.lastReadTs = System.currentTimeMillis();
        }

        private T read(boolean advance) {
            T val = readCached(advance);
            if (val != null) {
                return val;
            }

            try (BufferRef ref = dataStream.bulkRead(storage, bufferPool, Direction.FORWARD, readPosition)) {
                int[] entriesLength = ref.readAllInto(pageQueue, serializer);
                for (int length : entriesLength) {
                    entriesSizes.add(length);
                }

                return entriesLength.length > 0 ? readCached(advance) : null;
            }
        }

        private T readCached(boolean advance) {
            T val = advance ? pageQueue.poll() : pageQueue.peek();
            if (val != null) {
                int len = advance ? entriesSizes.poll() : entriesSizes.peek();
                if (advance) {
                    readPosition += len;
                }
            }
            return val;
        }

        private synchronized T tryTake(long sleepInterval, TimeUnit timeUnit, boolean advance) throws InterruptedException {
            if (hasDataAvailable()) {
                this.lastReadTs = System.currentTimeMillis();
                return read(true);
            }
            waitForData(sleepInterval, timeUnit);
            this.lastReadTs = System.currentTimeMillis();
            return read(advance);
        }

        private boolean hasDataAvailable() {
            if (!pageQueue.isEmpty()) {
                return true;
            }
            if (Segment.this.readOnly()) {
                return readPosition < Segment.this.header.logEnd;
            }
            return readPosition < Segment.this.position();
        }

        private synchronized T tryPool(long time, TimeUnit timeUnit) throws InterruptedException {
            if (hasDataAvailable()) {
                this.lastReadTs = System.currentTimeMillis();
                return read(true);
            }
            if (time > 0) {
                waitFor(time, timeUnit);
            }
            this.lastReadTs = System.currentTimeMillis();
            return read(true);
        }

        private void waitFor(long time, TimeUnit timeUnit) throws InterruptedException {
            long elapsed = 0;
            long start = System.currentTimeMillis();
            long maxWaitTime = timeUnit.toMillis(time);
            long interval = Math.min(maxWaitTime, VERIFICATION_INTERVAL_MILLIS);
            while (!closed.get() && !hasDataAvailable() && elapsed < maxWaitTime) {
                TimeUnit.MILLISECONDS.sleep(interval);
                elapsed = System.currentTimeMillis() - start;
            }
        }

        private void waitForData(long time, TimeUnit timeUnit) throws InterruptedException {
            while (!closed.get() && !readOnly() && !hasDataAvailable()) {
                timeUnit.sleep(time);
                this.lastReadTs = System.currentTimeMillis();
            }
        }

        @Override
        public T peek() throws InterruptedException {
            return tryTake(VERIFICATION_INTERVAL_MILLIS, TimeUnit.MILLISECONDS, false);
        }

        @Override
        public T poll() throws InterruptedException {
            return tryPool(NO_SLEEP, TimeUnit.MILLISECONDS);
        }

        @Override
        public T poll(long time, TimeUnit timeUnit) throws InterruptedException {
            return tryPool(time, timeUnit);
        }

        @Override
        public T take() throws InterruptedException {
            return tryTake(VERIFICATION_INTERVAL_MILLIS, TimeUnit.MILLISECONDS, true);
        }

        @Override
        public boolean headOfLog() {
            if (Segment.this.readOnly()) {
                return readPosition >= Segment.this.header.logEnd;
            }
            return readPosition == Segment.this.position();
        }

        @Override
        public boolean endOfLog() {
            return readOnly() && readPosition >= header.logEnd;

        }

        @Override
        public long position() {
            return readPosition;
        }

        @Override
        public void close() {
            Segment.this.removeFromReaders(this);
        }

        @Override
        public String toString() {
            return "SegmentPoller{readPosition=" + readPosition +
                    ", lastReadTs=" + lastReadTs +
                    '}';
        }
    }

}
