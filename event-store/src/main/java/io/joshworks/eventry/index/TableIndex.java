package io.joshworks.eventry.index;

import io.joshworks.eventry.index.disk.IndexAppender;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.LogPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;

public class TableIndex implements Index {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);
    public static final int DEFAULT_FLUSH_THRESHOLD = 1000000;
    public static final boolean DEFAULT_USE_COMPRESSION = false;
    private final int flushThreshold; //TODO externalize

    private final IndexAppender diskIndex;
    private MemIndex memIndex = new MemIndex();

    private final Set<IndexPoller> pollers = new HashSet<>();

    public TableIndex(File rootDirectory) {
        this(rootDirectory, DEFAULT_FLUSH_THRESHOLD, DEFAULT_USE_COMPRESSION);
    }

    TableIndex(File rootDirectory, int flushThreshold, boolean useCompression) {
        if (flushThreshold < 1000) {//arbitrary number
            throw new IllegalArgumentException("Flush threshold must be at least 1000");
        }

        this.diskIndex = new IndexAppender(rootDirectory, flushThreshold * IndexEntry.BYTES, flushThreshold, useCompression);
        this.flushThreshold = flushThreshold;
    }

    //returns true if flushed to disk
    public FlushInfo add(long stream, int version, long position) {
        if (version <= IndexEntry.NO_VERSION) {
            throw new IllegalArgumentException("Version must be greater than or equals to zero");
        }
        if (position < 0) {
            throw new IllegalArgumentException("Position must be greater than zero");
        }
        IndexEntry entry = IndexEntry.of(stream, version, position);
        FlushInfo flushInfo = null;
        if (memIndex.size() >= flushThreshold) {
            flushInfo = writeToDisk();
        }
        memIndex.add(entry);
        return flushInfo;
    }

    //only single write can happen at time
    private FlushInfo writeToDisk() {
        logger.info("Writing index to disk");

        long start = System.currentTimeMillis();
        diskIndex.writeToDisk(memIndex);
        long timeTaken = System.currentTimeMillis() - start;
        logger.info("Index write took {}ms", timeTaken);

        memIndex.close();
        memIndex = new MemIndex();

        return new FlushInfo(memIndex.size(), timeTaken);
    }

    @Override
    public int version(long stream) {
        int version = memIndex.version(stream);
        if (version > IndexEntry.NO_VERSION) {
            return version;
        }

        return diskIndex.version(stream);
    }

    public long size() {
        return diskIndex.entries() + memIndex.size();
    }

    @Override
    public void close() {
//        this.flush(); //no need to flush, just reload from disk on startup
        memIndex.close();
        diskIndex.close();
        for (IndexPoller poller : pollers) {
            IOUtils.closeQuietly(poller);
        }
        pollers.clear();

    }

    @Override
    public LogIterator<IndexEntry> iterator(Direction direction, Range range) {
        LogIterator<IndexEntry> cacheIterator = memIndex.iterator(direction, range);
        LogIterator<IndexEntry> diskIterator = diskIndex.iterator(direction, range);

        return joiningDiskAndMem(diskIterator, cacheIterator);
    }

    @Override
    public Stream<IndexEntry> stream(Direction direction, Range range) {
        return Iterators.closeableStream(iterator(direction, range));
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Optional<IndexEntry> fromMemory = memIndex.get(stream, version);
        if (fromMemory.isPresent()) {
            return fromMemory;
        }
        return diskIndex.get(stream, version);
    }

    private LogIterator<IndexEntry> joiningDiskAndMem(LogIterator<IndexEntry> diskIterator, LogIterator<IndexEntry> memIndex) {
        return Iterators.concat(Arrays.asList(diskIterator, memIndex));
    }

    public FlushInfo flush() {
        return writeToDisk();
    }

    public void compact() {
        diskIndex.compact();
    }

    public IndexPoller poller(long stream) {
        return poller(Set.of(stream));
    }

    public IndexPoller poller(Map<Long, Integer> streams) {
        Map<Long, AtomicInteger> map = streams.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, kv -> new AtomicInteger(kv.getValue())));
        return new IndexPoller(map);
    }

    public IndexPoller poller(Set<Long> streams) {
        List<Long> streamList = new ArrayList<>(streams);
        streamList.sort(Comparator.comparingLong(s -> s));
        Map<Long, AtomicInteger> map = streamList.stream().collect(Collectors.toMap(stream -> stream, r -> new AtomicInteger(NO_VERSION)));
        return new IndexPoller(map);
    }


    public class IndexPoller implements LogPoller<IndexEntry> {

        private static final int MAX_BACKOFF_COUNT = 6;
        private static final int INITIAL_WAIT_TIME = 200;
        private final Map<Long, AtomicInteger> streams = new ConcurrentHashMap<>();
        private final BlockingQueue<IndexEntry> queue = new LinkedBlockingQueue<>();
        private final Queue<Long> streamReadPriority;
        private final AtomicBoolean closed = new AtomicBoolean();

        private IndexPoller(Map<Long, AtomicInteger> streamVersions) {
            streams.putAll(streamVersions);
            streamReadPriority = new ArrayDeque<>(streamVersions.keySet());
        }

        private IndexEntry computeAndGet(IndexEntry ie) {
            if (ie == null) {
                return null;
            }
            AtomicInteger lastVersion = streams.get(ie.stream);
            if (lastVersion.get() >= ie.version) {
                throw new IllegalStateException("Reading already processed version, last processed version: " + lastVersion + " read version: " + ie.version);
            }
            int expected = lastVersion.get() + 1;
            if (expected != ie.version) {
                throw new IllegalStateException("Next expected version: " + expected + " got: " + ie.version);
            }
            lastVersion.incrementAndGet();
            return ie;
        }

        private void tryFetch() {
            if (queue.isEmpty()) {
                Queue<Long> emptyStreams = new ArrayDeque<>();
                while (!streamReadPriority.isEmpty()) {
                    long stream = streamReadPriority.peek();
                    int lastProcessedVersion = streams.get(stream).get();
                    List<IndexEntry> indexEntries = blockGet(stream, lastProcessedVersion + 1);
                    if (!indexEntries.isEmpty()) {
                        queue.addAll(indexEntries);
                        streamReadPriority.addAll(emptyStreams);
                        return;
                    } else {
                        streamReadPriority.poll();
                        emptyStreams.offer(stream);
                    }
                }
                streamReadPriority.addAll(emptyStreams);
            }
        }

        private List<IndexEntry> blockGet(long stream, int version) {
            List<IndexEntry> fromDisk = diskIndex.getBlockEntries(stream, version);
            List<IndexEntry> filtered = filtering(fromDisk);
            if (!filtered.isEmpty()) {
                return filtered;
            }
            List<IndexEntry> fromMemory = memIndex.getAllOf(stream);
            return filtering(fromMemory);
        }

        private List<IndexEntry> filtering(List<IndexEntry> original) {
            if (original.isEmpty()) {
                return Collections.emptyList();
            }
            return original.stream().filter(ie -> {
                AtomicInteger version = streams.get(ie.stream);
                return version != null && ie.version > version.get();
            }).collect(Collectors.toList());
        }


        @Override
        public synchronized IndexEntry peek() {
            tryFetch();
            return queue.peek();
        }

        @Override
        public synchronized IndexEntry poll() {
            tryFetch();
            IndexEntry entry = queue.poll();
            return computeAndGet(entry);
        }

        @Override
        public synchronized IndexEntry poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            tryFetch();
            IndexEntry entry = queue.poll(limit, timeUnit);
            if (entry == null) {
                tryFetch();
                entry = queue.poll();
            }
            return computeAndGet(entry);
        }

        @Override
        public synchronized IndexEntry take() throws InterruptedException {
            IndexEntry entry;
            int waitingTimeMs = INITIAL_WAIT_TIME;
            int backoffCount = 0;
            do {
                tryFetch();
                entry = queue.poll(waitingTimeMs, TimeUnit.MILLISECONDS);
                waitingTimeMs = backoffCount++ > MAX_BACKOFF_COUNT ? waitingTimeMs : waitingTimeMs * 2;
            } while (entry == null && !closed.get());
            return computeAndGet(entry);
        }

        @Override
        public synchronized boolean headOfLog() {
            tryFetch();
            return queue.isEmpty();
        }

        @Override
        public boolean endOfLog() {
            return false;
        }

        @Override
        public long position() {
            return -1;
        }

        @Override
        public void close() {
            closed.set(true);
        }

        public synchronized Map<Long, Integer> processed() {
            return streams.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, kv -> kv.getValue().get()));
        }

    }

    public class FlushInfo {
        public int entries;
        public final long timeTaken;

        private FlushInfo(int entries, long timeTaken) {
            this.entries = entries;
            this.timeTaken = timeTaken;
        }
    }
}