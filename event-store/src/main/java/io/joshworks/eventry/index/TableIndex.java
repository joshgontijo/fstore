package io.joshworks.eventry.index;

import io.joshworks.eventry.index.disk.IndexAppender;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableIndex implements Index {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);
    public static final int DEFAULT_FLUSH_THRESHOLD = 1000000;
    public static final boolean DEFAULT_USE_COMPRESSION = false;
    private final int flushThreshold; //TODO externalize

    private final IndexAppender diskIndex;
    private MemIndex memIndex = new MemIndex();

    private final Set<DiskMemIndexPoller> pollers = new HashSet<>();

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

        for (DiskMemIndexPoller poller : pollers) {
            poller.onMemIndexFlushed();
        }

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
        for (DiskMemIndexPoller poller : pollers) {
            IOUtils.closeQuietly(poller);
        }
        pollers.clear();

    }

    @Override
    public LogIterator<IndexEntry> indexIterator(Direction direction) {
        LogIterator<IndexEntry> cacheIterator = memIndex.indexIterator(direction);
        LogIterator<IndexEntry> diskIterator = diskIndex.indexIterator(direction);

        return joiningDiskAndMem(diskIterator, cacheIterator);
    }

    @Override
    public LogIterator<IndexEntry> indexIterator(Direction direction, Range range) {
        LogIterator<IndexEntry> cacheIterator = memIndex.indexIterator(direction, range);
        LogIterator<IndexEntry> diskIterator = diskIndex.indexIterator(direction, range);

        return joiningDiskAndMem(diskIterator, cacheIterator);
    }

    @Override
    public Stream<IndexEntry> indexStream(Direction direction) {
        return Iterators.closeableStream(indexIterator(direction));
    }

    @Override
    public Stream<IndexEntry> indexStream(Direction direction, Range range) {
        return Iterators.closeableStream(indexIterator(direction, range));
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

    public PollingSubscriber<IndexEntry> poller(long stream) {
        return poller(Set.of(stream));
    }

    //TODO how to mark last read indexEntry ?
    //timestamp / position (how about the mem items ?) / version (of each stream individually, then crash could cause repeated)
    public PollingSubscriber<IndexEntry> poller(long stream, int lastVersion) {
        throw new UnsupportedOperationException("Implement me");
//        return poller(Set.of(stream));
    }

    public PollingSubscriber<IndexEntry> poller(Set<Long> streams) {
        return new StreamTrackerPoller(new DiskMemIndexPoller(diskIndex.poller()), streams);
    }

    /**
     * Higher level poller, that tracks the read streams, used in conjunction with DiskMemPoller to avoid duplicates
     * when switching from memindex to diskindex pollers and vice versa
     */
    private class StreamTrackerPoller implements PollingSubscriber<IndexEntry> {

        private final PollingSubscriber<IndexEntry> poller;
        private final Map<Long, Integer> streamsRead;
        private IndexEntry lastEntry;

        //TODO add support to restart from a set of streams / versions. map entries must be set with last read version of each stream
        //AND the disk memDiskPoller must have the position (already supported by log appender)
        StreamTrackerPoller(PollingSubscriber<IndexEntry> memDiskPoller, Set<Long> streams) {
            this.poller = memDiskPoller;
            this.streamsRead = streams.stream().collect(Collectors.toMap(aLong -> aLong, aLong -> -1));
        }

        private IndexEntry poolAndUpdateMap() throws InterruptedException {
            while (!poller.headOfLog()) {
                IndexEntry indexEntry = poller.poll();
                if (indexEntry == null) {
                    return null;
                }
                if (streamMatch(indexEntry)) {
                    streamsRead.computeIfPresent(indexEntry.stream, (k, v) -> indexEntry.version);
                    lastEntry = indexEntry;
                    return indexEntry;
                }
            }
            return null;
        }

        private boolean streamMatch(IndexEntry indexEntry) {
            return streamsRead.getOrDefault(indexEntry.stream, Integer.MAX_VALUE) < indexEntry.version;
        }

        @Override
        public synchronized IndexEntry peek() throws InterruptedException {
            if (lastEntry != null) {
                return lastEntry;
            }
            IndexEntry indexEntry = poller.peek();
            if (indexEntry == null || !streamMatch(indexEntry)) {
                return null;
            }
            lastEntry = indexEntry;
            return indexEntry;
        }

        @Override
        public synchronized IndexEntry poll() throws InterruptedException {
            return poll(-1, TimeUnit.SECONDS);
        }

        @Override
        public synchronized IndexEntry poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            IndexEntry indexEntry = poolAndUpdateMap();
            if (indexEntry != null) {
                lastEntry = null; //invalidate future peek
            }
            return indexEntry;
        }

        @Override
        public synchronized IndexEntry take() throws InterruptedException {
            IndexEntry indexEntry;
            do {
                indexEntry = poller.take();

            } while (indexEntry == null || !streamMatch(indexEntry));
            streamsRead.computeIfPresent(indexEntry.stream, (k, v) -> v + 1);

            lastEntry = null; //invalidate future peek
            return indexEntry;
        }

        @Override
        public synchronized boolean headOfLog() {
            return poller.headOfLog();
        }

        @Override
        public synchronized boolean endOfLog() {
            return poller.endOfLog();
        }

        @Override
        public long position() {
            return poller.position();
        }

        @Override
        public synchronized void close() throws IOException {
            poller.close();
            //TODO store state ??
        }
    }

    /**
     * Manages the MemIndex and DiskIndex pollers
     * Start polling from disk if data is available.
     * Switches to mem poller if no entry is available in disk
     * Once the disk is flushed and the current poller is on mem, switch to disk and discard the mem poller
     * When catching up from disk, possible duplicate entry can be polled since the reordering on disk write
     * MUST NOT BE USED WITHOUT StreamIndexPoller that manages the already read items
     */
    private class DiskMemIndexPoller implements PollingSubscriber<IndexEntry> {

        private final PollingSubscriber<IndexEntry> diskPoller;
        private PollingSubscriber<IndexEntry> memPoller;
        private PollingSubscriber<IndexEntry> current;
        private boolean memPolling;

        private DiskMemIndexPoller(PollingSubscriber<IndexEntry> diskPoller) {
            this.diskPoller = diskPoller;
            this.memPoller = newMemPoller();
            this.current = diskPoller;
        }

        private void checkSwitchRequired() {
            if (!memPolling && diskPoller.headOfLog()) {
                current = newMemPoller();
                memPolling = true;
            }
            if (memPolling && !diskPoller.headOfLog()) {
                current = diskPoller;
                memPolling = false;
            }
        }

        @Override
        public synchronized IndexEntry peek() throws InterruptedException {
            checkSwitchRequired();
            return current.peek();
        }

        @Override
        public synchronized IndexEntry poll() throws InterruptedException {
            checkSwitchRequired();
            return current.poll();
        }


        @Override
        public synchronized IndexEntry poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            checkSwitchRequired();
            return current.poll(limit, timeUnit);
        }

        @Override
        public synchronized IndexEntry take() throws InterruptedException {
            checkSwitchRequired();
            IndexEntry take;
            //take might be null if mem index gets closed
            while ((take = current.take()) == null) {
                checkSwitchRequired();
            }
            return take;
        }

        @Override
        public synchronized boolean headOfLog() {
            return diskPoller.headOfLog() && memPoller.headOfLog();
        }

        @Override
        public boolean endOfLog() {
            return false;
        }

        @Override
        public long position() {
            //always use disk position, on restart duplicate might be read but the tracker poller will remove then
            return diskPoller.position();
        }

        @Override
        public synchronized void close() {
            IOUtils.closeQuietly(diskPoller);
            IOUtils.closeQuietly(memPoller);
        }

        private synchronized PollingSubscriber<IndexEntry> newMemPoller() {
            IOUtils.closeQuietly(memPoller);
            return memIndex.poller();
        }

        //releases memindex pollers and acquire new one
        private synchronized void onMemIndexFlushed() {
            memPoller = newMemPoller();
            memPolling = false;
            current = diskPoller;
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