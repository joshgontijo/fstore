package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.seda.EventContext;
import io.joshworks.fstore.core.seda.SedaContext;
import io.joshworks.fstore.core.seda.Stage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.compaction.Compactor;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.log.utils.BitUtil;
import io.joshworks.fstore.log.utils.LogFileUtils;
import io.joshworks.fstore.log.utils.Logging;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Position address schema
 * <p>
 * |------------ 64bits -------------|
 * [SEGMENT_IDX] [POSITION_ON_SEGMENT]
 * <p>
 * *
 * Position address schema (BLock)
 * <p>
 * |------------------ 64bits -------------------|
 * [SEGMENT_IDX] [POSITION_ON_SEGMENT] [BLOCK_POS]
 */

public class LogAppender<T> implements Closeable {

    private final Logger logger;


    private static final int SEGMENT_BITS = 16;
    private static final int SEGMENT_ADDRESS_BITS = Long.SIZE - SEGMENT_BITS;

    static final long MAX_SEGMENTS = BitUtil.maxValueForBits(SEGMENT_BITS);
    static final long MAX_SEGMENT_ADDRESS = BitUtil.maxValueForBits(SEGMENT_ADDRESS_BITS);

    private final File directory;
    private final Serializer<T> serializer;
    private final Metadata metadata;
    private final IDataStream dataStream;
    private final NamingStrategy namingStrategy;
    private final SegmentFactory<T> factory;
    private final StorageProvider storageProvider;

    final Levels<T> levels;

    //state
    private final State state;
    private final boolean compactionDisabled;

    private AtomicBoolean closed = new AtomicBoolean();

    private final ScheduledExecutorService flushWorker;
    private final SedaContext sedaContext = new SedaContext();
    private final Set<LogPoller> pollers = new HashSet<>();
    private final Compactor<T> compactor;

    public static <T> Config<T> builder(File directory, Serializer<T> serializer) {
        return new Config<>(directory, serializer);
    }

    LogAppender(Config<T> config) {
        this.directory = config.directory;
        this.serializer = config.serializer;
        this.factory = config.segmentFactory;
        this.storageProvider = StorageProvider.of(config.mode);
        this.namingStrategy = config.namingStrategy;
        this.dataStream = new DataStream(config.bufferPool, config.checksumProbability);
        this.compactionDisabled = config.compactionDisabled;
        this.logger = Logging.namedLogger(config.name, "appender");

        boolean metadataExists = LogFileUtils.metadataExists(directory);

        if (!metadataExists) {
            logger.info("Creating LogAppender");

            if (config.segmentSize > MAX_SEGMENT_ADDRESS) {
                throw new IllegalArgumentException("Maximum segment size allowed is " + MAX_SEGMENT_ADDRESS);
            }

            LogFileUtils.createRoot(directory);
            this.metadata = Metadata.write(directory, config.segmentSize, config.compactionThreshold, config.flushMode);

            this.state = State.empty(directory);
        } else {
            logger.info("Opening LogAppender");
            this.metadata = Metadata.readFrom(directory);
            this.state = State.readFrom(directory);
        }

        if (FlushMode.PERIODICALLY.equals(metadata.flushMode)) {
            this.flushWorker = Executors.newSingleThreadScheduledExecutor();
            this.flushWorker.scheduleWithFixedDelay(this::flush, 5, 5, TimeUnit.SECONDS);
        } else {
            this.flushWorker = null;
        }

        this.levels = loadLevels();
        this.compactor = new Compactor<>(directory, config.combiner, factory, storageProvider, serializer, dataStream, namingStrategy, metadata.compactionThreshold, metadata.magic, config.name, levels, sedaContext, config.threadPerLevel);
        logConfig(config);

        sedaContext.addStage("WRITE", this::appendInternal2, new Stage.Builder().corePoolSize(1).maximumPoolSize(1));
    }

    private void logConfig(Config<T> config) {
        logger.info("STORAGE LOCATION: {}", config.directory.toPath());
        logger.info("COMPACTION ENABLED: {}", !config.compactionDisabled);
        logger.info("SEGMENT BITS : {}", SEGMENT_BITS);
        logger.info("MAX SEGMENTS: {} ({} bits)", MAX_SEGMENTS, SEGMENT_BITS);
        logger.info("MAX SEGMENT ADDRESS: {} ({} bits)", MAX_SEGMENT_ADDRESS, SEGMENT_ADDRESS_BITS);

        logger.info("BUFFER POOL: {}", config.bufferPool.getClass().getSimpleName());
        logger.info("SEGMENT SIZE: {}", config.segmentSize);
        logger.info("FLUSH MODE: {}", config.flushMode);
        logger.info("COMPACTION ENABLED: {}", !this.compactionDisabled);
        logger.info("COMPACTION THRESHOLD: {}", config.compactionThreshold);
        logger.info("STORAGE MODE: {}", config.mode);
    }

    private Levels<T> loadLevels() {
        try {
            Levels<T> loadedLevels = loadSegments();
            restoreState(loadedLevels);
            return loadedLevels;
        } catch (Exception e) {
            IOUtils.closeQuietly(state);
            throw e;
        }
    }

    private void restoreState(Levels<T> levels) {
        Log<T> current = levels.current();
        logger.info("Restoring state");
        long segmentPosition = current.position();
        long position = toSegmentedPosition(levels.numSegments() - 1L, segmentPosition);
        state.position(position);
        state.addEntryCount(current.entries());

        logger.info("State restored: {}", state);
    }

    private Log<T> createCurrentSegment() {
        File segmentFile = LogFileUtils.newSegmentFile(directory, namingStrategy, 1);
        Storage storage = storageProvider.create(segmentFile, metadata.segmentSize);
        return factory.createOrOpen(storage, serializer, dataStream, metadata.magic, Type.LOG_HEAD);
    }

    private Levels<T> loadSegments() {

        List<Log<T>> segments = new ArrayList<>();
        try {
            for (String segmentName : LogFileUtils.findSegments(directory)) {
                Log<T> segment = loadSegment(segmentName);
                if (Type.MERGE_OUT.equals(segment.type()) || Type.DELETED.equals(segment.type())) {
                    logger.info("Deleting dangling segment: {}", segment);
                    segment.delete();
                    continue;
                }
                segments.add(segment);
            }

            long levelZeroSegments = segments.stream().filter(l -> l.level() == 0).count();

            if (levelZeroSegments == 0) {
                //create current segment
                Log<T> currentSegment = createCurrentSegment();
                segments.add(currentSegment);
            }
            if (levelZeroSegments > 1) {
                throw new IllegalStateException("Multiple level zero segments");
            }

        } catch (Exception e) {
            segments.forEach(IOUtils::closeQuietly);
            throw e;
        }


        return Levels.create(segments);
    }

    private Log<T> loadSegment(String segmentName) {
        Storage storage = null;
        try {
            File segmentFile = LogFileUtils.getSegmentHandler(directory, segmentName);
            storage = storageProvider.open(segmentFile);
            Log<T> segment = factory.createOrOpen(storage, serializer, dataStream, metadata.magic, null);
            logger.info("Loaded segment {}", segment);
            return segment;
        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw e;
        }
    }

    public synchronized void roll() {
        try {
            Log<T> current = levels.current();
            if (FlushMode.ON_ROLL.equals(metadata.flushMode)) {
                current.flush();
            }
            logger.info("Rolling segment: {}", current);
            if (current.entries() == 0) {
                logger.warn("No entries in the current segment");
                return;
            }

            current.roll(1);

            Log<T> newSegment = createCurrentSegment();
            levels.appendSegment(newSegment);

            notifyPollers(newSegment);

            state.lastRollTime(System.currentTimeMillis());
            state.flush();

            if (!compactionDisabled) {
                compactor.requestCompaction(1);
            }


        } catch (Exception e) {
            throw new RuntimeIOException("Could not roll segment file", e);
        }
    }

    int getSegment(long position) {
        long segmentIdx = (position >>> SEGMENT_ADDRESS_BITS);
        if (segmentIdx > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Invalid segment, value cannot be greater than " + MAX_SEGMENTS);
        }

        return (int) segmentIdx;
    }

    long toSegmentedPosition(long segmentIdx, long position) {
        if (segmentIdx < 0) {
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (segmentIdx > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + MAX_SEGMENTS);
        }
        return (segmentIdx << SEGMENT_ADDRESS_BITS) | position;
    }

    long getPositionOnSegment(long position) {
        long mask = (1L << SEGMENT_ADDRESS_BITS) - 1;
        return (position & mask);
    }

    public void compact() {
        compactor.forceCompaction(1);
    }

    private void notifyPollers(Log<T> newSegment) {
        for (LogPoller poller : pollers) {
            poller.addSegment(newSegment);
        }
    }

    private final Semaphore semaphore = new Semaphore(10000, true);
    private final ExecutorService writer = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("writer");
        return t;
    });


    public long append(T data) {
        if(closed.get()) {
            throw new AppendException("Stored closed");
        }
        try {
            return (long) sedaContext.submit("WRITE", data).get();
//            return appendAsync(data).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AppendException(e);
        } catch (ExecutionException e) {
            throw new AppendException(e);
        }
    }

    public CompletableFuture<Long> appendAsync(T data) {
        return CompletableFuture.supplyAsync(() -> this.appendInternal(data), writer);
    }

    private void appendInternal2(EventContext<T> ctx) {
        long pos = appendInternal(ctx.data);
        ctx.complete(pos);
    }

    private long appendInternal(T data) {
        if(closed.get()) {
            throw new AppendException("Stored closed");
        }
        semaphore.acquireUninterruptibly();
        try {
            Log<T> current = levels.current();

            int segments = levels.numSegments();
            long positionOnSegment = current.append(data);
            if (current.logicalSize() > metadata.segmentSize) {
                roll();
            }
            if (FlushMode.ALWAYS.equals(metadata.flushMode)) {
                flush();
            }
            long entryPosition = toSegmentedPosition(segments - 1L, positionOnSegment);
            long currentPosition = toSegmentedPosition(segments - 1L, current.position());

            state.position(currentPosition);
            state.incrementEntryCount();
            return entryPosition;
        }finally {
            semaphore.release();
        }
    }

    public String name() {
        return directory.getName();
    }

    public LogIterator<T> iterator(Direction direction) {
        long startPosition = Direction.FORWARD.equals(direction) ? Log.START : Math.max(position(), Log.START);
        return iterator(startPosition, direction);
    }

    public Stream<T> stream(Direction direction) {
        return Iterators.closeableStream(iterator(direction));
    }

    public synchronized LogIterator<T> iterator(long position, Direction direction) {
        return Direction.FORWARD.equals(direction) ? new ForwardLogReader(position) : new BackwardLogReader(position);
    }

    public PollingSubscriber<T> poller() {
        return createPoller(Log.START);
    }

    public PollingSubscriber<T> poller(long position) {
        return createPoller(position);
    }

    private synchronized PollingSubscriber<T> createPoller(long position) {
        LogPoller logPoller = new LogPoller(position);
        pollers.add(logPoller);
        return logPoller;
    }


    public long position() {
        return state.position();
    }

    public T get(long position) {
        int segmentIdx = getSegment(position);
        validateSegmentIdx(segmentIdx, position);

        long positionOnSegment = getPositionOnSegment(position);
        Log<T> segment = levels.get(segmentIdx);
        if (segment != null) {
            return segment.get(positionOnSegment);
        }
        return null;
    }

    private void validateSegmentIdx(int segmentIdx, long pos) {
        if (segmentIdx < 0 || segmentIdx > levels.numSegments()) {
            throw new IllegalArgumentException("No segment for address " + pos + " (segmentIdx: " + segmentIdx + "), available segments: " + levels.numSegments());
        }
    }

    public long size() {
        return Iterators.closeableStream(levels.segments(Direction.FORWARD)).mapToLong(Log::fileSize).sum();
    }

    public long size(int level) {
        return segments(level).stream().mapToLong(Log::fileSize).sum();
    }

    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        logger.info("Closing log appender {}", directory.getName());
//        stateScheduler.shutdown();

        writer.shutdown();
        sedaContext.shutdown();

        Log<T> currentSegment = levels.current();
        if (currentSegment != null) {
            currentSegment.flush();
            state.position(this.position());
        }

//        state.flush();
        state.close();
        closeSegments();

        for (LogPoller poller : pollers) {
            poller.close();
        }
    }

    private void closeSegments() {
        try (LogIterator<Log<T>> segments = levels.segments(Direction.FORWARD)) {
            while (segments.hasNext()) {
                Log<T> segment = segments.next();
                logger.info("Closing segment {}", segment.name());
                IOUtils.closeQuietly(segment);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to close log appender", e);
        }
    }

    public synchronized void flush() {
        if (closed.get()) {
            return;
        }
        levels.current().flush();
    }

    public long entries() {
        return this.state.entryCount();
    }

    public String currentSegment() {
        return levels.current().name();
    }

    public Log<T> current() {
        return levels.current();
    }

    public <R> R acquireSegments(Direction direction, Function<LogIterator<Log<T>>, R> function) {
        int retries = 0;
        LogIterator<Log<T>> segments = null;
        do {
            try {
                segments = levels.segments(direction);
            } catch (Exception e) {
                logger.warn("Failed to acquire segments", e);
                if (retries++ > 10) {
                    throw new RuntimeException("Failed to acquire segment after " + retries + " retries");
                }
            }
        } while (segments == null);
        return function.apply(segments);
    }

    private List<Log<T>> segments(int level) {
        if (level < 0) {
            throw new IllegalArgumentException("Level must be at least zero");
        }
        if (level > levels.depth()) {
            throw new IllegalArgumentException("No such level " + level + ", current depth: " + levels.depth());
        }
        return levels.segments(level);
    }

    public int depth() {
        return levels.depth();
    }

    public Path directory() {
        return directory.toPath();
    }


    //FORWARD SCAN: When at the end of a segment, advance to the next, so the current position is correct
    //----
    //BACKWARD SCAN: At the beginning of a segment, do not move to previous until next is called.
    // hasNext calls will always return true, since the previous segment always has data
    private class ForwardLogReader implements LogIterator<T> {

        private final Iterator<LogIterator<T>> segmentsIterators;
        private LogIterator<T> current;
        private int segmentIdx;

        ForwardLogReader(long startPosition) {
            //TODO this might throw exception on acquiring a reader
            Iterator<Log<T>> segments = levels.segments(Direction.FORWARD);
            this.segmentIdx = getSegment(startPosition);

            validateSegmentIdx(segmentIdx, startPosition);
            long positionOnSegment = getPositionOnSegment(startPosition);

            // skip
            for (int i = 0; i < this.segmentIdx; i++) {
                segments.next();
            }

            if (segments.hasNext()) {
                this.current = segments.next().iterator(positionOnSegment, Direction.FORWARD);
            }

            List<LogIterator<T>> subsequentIterators = new ArrayList<>();
            while (segments.hasNext()) {
                subsequentIterators.add(segments.next().iterator(Direction.FORWARD));
            }
            this.segmentsIterators = subsequentIterators.iterator();

        }

        @Override
        public long position() {
            return toSegmentedPosition(segmentIdx, current.position());
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                return false;
            }
            boolean hasNext = current.hasNext();
            if (!hasNext) {
                IOUtils.closeQuietly(current);
                if (!segmentsIterators.hasNext()) {
                    return false;
                }
                current = segmentsIterators.next();
                segmentIdx++;
                return current.hasNext();
            }
            return true;
        }

        @Override
        public T next() {
            T next = current.next();
            if ((next == null || !current.hasNext())) {
                IOUtils.closeQuietly(current);
                if (segmentsIterators.hasNext()) {
                    current = segmentsIterators.next();
                    segmentIdx++;
                }
            }
            return next;
        }

        @Override
        public void close() {
            try {
                current.close();
                while (segmentsIterators.hasNext()) {
                    segmentsIterators.next().close();
                }
            } catch (IOException e) {
                throw RuntimeIOException.of(e);
            }
        }
    }

    private class BackwardLogReader implements LogIterator<T> {

        private final Iterator<LogIterator<T>> segmentsIterators;
        private LogIterator<T> current;
        private int segmentIdx;

        BackwardLogReader(long startPosition) {
            int numSegments = levels.numSegments();
            Iterator<Log<T>> segments = levels.segments(Direction.BACKWARD);
            int segIdx = getSegment(startPosition);

            this.segmentIdx = numSegments - (numSegments - segIdx);
            int skips = (numSegments - 1) - segIdx;

            validateSegmentIdx(segmentIdx, startPosition);
            long positionOnSegment = getPositionOnSegment(startPosition);

            // skip
            for (int i = 0; i < skips; i++) {
                segments.next();
            }

            if (segments.hasNext()) {
                this.current = segments.next().iterator(positionOnSegment, Direction.BACKWARD);
            }

            List<LogIterator<T>> subsequentIterators = new ArrayList<>();
            while (segments.hasNext()) {
                subsequentIterators.add(segments.next().iterator(Direction.BACKWARD));
            }
            this.segmentsIterators = subsequentIterators.iterator();

        }

        @Override
        public long position() {
            return toSegmentedPosition(segmentIdx, current.position());
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                return false;
            }
            return current.hasNext() || segmentsIterators.hasNext();
        }

        @Override
        public T next() {
            if ((current == null || !current.hasNext()) && segmentsIterators.hasNext()) {
                IOUtils.closeQuietly(current);
                current = segmentsIterators.next();
                segmentIdx--;
            }
            if (current == null || !hasNext()) {
                return null; //TODO throw nosuchelementexception
            }
            return current.next();
        }

        @Override
        public void close() {
            try {
                current.close();
                while (segmentsIterators.hasNext()) {
                    segmentsIterators.next().close();
                }
            } catch (IOException e) {
                throw RuntimeIOException.of(e);
            }
        }
    }


    private class LogPoller implements PollingSubscriber<T> {

        private final BlockingQueue<PollingSubscriber<T>> segmentPollers = new LinkedBlockingQueue<>();
        private final int MAX_SEGMENT_WAIT_SEC = 5;
        private PollingSubscriber<T> currentPoller;
        private int segmentIdx;
        private final AtomicBoolean closed = new AtomicBoolean();

        LogPoller(long startPosition) {
            this.segmentIdx = getSegment(startPosition);
            validateSegmentIdx(segmentIdx, startPosition);
            Iterator<Log<T>> segments = levels.segments(Direction.FORWARD);
            long positionOnSegment = getPositionOnSegment(startPosition);

            // skip
            for (int i = 0; i <= segmentIdx - 1; i++) {
                segments.next();
            }

            if (segments.hasNext()) {
                this.currentPoller = segments.next().poller(positionOnSegment);
            }

            while (segments.hasNext()) {
                segmentPollers.add(segments.next().poller());
            }

        }

        @Override
        public T peek() throws InterruptedException {
            return peekData();
        }

        @Override
        public T poll() throws InterruptedException {
            return pollData(PollingSubscriber.NO_SLEEP, TimeUnit.SECONDS);
        }

        @Override
        public T poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            return pollData(limit, timeUnit);
        }

        @Override
        public T take() throws InterruptedException {
            return takeData();
        }

        private synchronized T pollData(long limit, TimeUnit timeUnit) throws InterruptedException {
            if (closed.get()) {
                return null;
            }
            T item = limit < 0 ? currentPoller.poll() : currentPoller.poll(limit, timeUnit);
            if (item == null && nextSegment())
                return pollData(limit, timeUnit);
            return item;
        }

        private boolean nextSegment() throws InterruptedException {
            if (currentPoller.endOfLog()) { //end of segment
                IOUtils.closeQuietly(this.currentPoller);
                this.currentPoller = waitForNextSegment();
                if (this.currentPoller == null) { //close was called
                    return false;
                }
                segmentIdx++;
                return true;
            }
            return false;
        }

        private synchronized T peekData() throws InterruptedException {
            if (closed.get()) {
                return null;
            }
            T item = currentPoller.peek();
            if (nextSegment() && item == null)
                return peek();
            return item;
        }

        private synchronized T takeData() throws InterruptedException {
            if (closed.get()) {
                return null;
            }
            T item = currentPoller.take();
            if (currentPoller.endOfLog()) { //end of segment
                IOUtils.closeQuietly(this.currentPoller);
                this.currentPoller = waitForNextSegment();

                if (this.currentPoller == null) { //close was called
                    return item;
                }
                segmentIdx++;
                if (item != null) {
                    return item;
                }
                return takeData();
            }
            return item;
        }

        private PollingSubscriber<T> waitForNextSegment() throws InterruptedException {
            PollingSubscriber<T> next = null;
            while (!closed.get() && next == null) {
                next = segmentPollers.poll(MAX_SEGMENT_WAIT_SEC, TimeUnit.SECONDS);//wait next segment, should never wait really
            }
            return next;
        }

        @Override
        public synchronized boolean headOfLog() {
            //if end of current segment, check the next one
            if (currentPoller.headOfLog()) {
                //TODO verify if the !segmentPollers.isEmpty() is actually correct and is a replacement for the commented out code below
//                boolean isLatestSegment = segmentIdx == levels.numSegments() - 1;
                if (!segmentPollers.isEmpty()) {
                    PollingSubscriber<T> poll = segmentPollers.peek();
                    return poll.headOfLog();
                }
                return true;
            }
            return false;
        }

        @Override
        public boolean endOfLog() {
            return false;
        }

        @Override
        public long position() {
            return toSegmentedPosition(segmentIdx, currentPoller.position());
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            for (PollingSubscriber<T> poller : segmentPollers) {
                IOUtils.closeQuietly(poller);
            }

            segmentPollers.clear();
            LogAppender.this.pollers.remove(this);
            IOUtils.closeQuietly(currentPoller);
        }

        private void addSegment(Log<T> segment) {
            segmentPollers.add(segment.poller());
        }

    }

}
