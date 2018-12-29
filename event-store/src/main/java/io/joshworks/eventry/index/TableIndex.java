package io.joshworks.eventry.index;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.index.disk.IndexAppender;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;

public class TableIndex implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);
    public static final int DEFAULT_FLUSH_THRESHOLD = 1000000;
    public static final boolean DEFAULT_USE_COMPRESSION = false;
    private final int flushThreshold; //TODO externalize

    private final IndexAppender diskIndex;
    private MemIndex memIndex = new MemIndex();

    private final Set<IndexIterator> pollers = new HashSet<>();

    public TableIndex(File rootDirectory) {
        this(rootDirectory, DEFAULT_FLUSH_THRESHOLD, DEFAULT_USE_COMPRESSION);
    }

    TableIndex(File rootDirectory, int flushThreshold, boolean useCompression) {
        this.diskIndex = new IndexAppender(rootDirectory, flushThreshold * IndexEntry.BYTES, flushThreshold, useCompression);
        this.flushThreshold = flushThreshold;
    }

    public FlushInfo add(StreamName stream, long position) {
        return add(stream.hash(), stream.version(), position);
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
        memIndex.add(entry);

        if (memIndex.size() >= flushThreshold) {
            return flush();
        }
        return null;
    }

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

    public void close() {
//        this.flush(); //no need to flush, just reload from disk on startup
        memIndex.close();
        diskIndex.close();
        for (IndexIterator poller : pollers) {
            IOUtils.closeQuietly(poller);
        }
        pollers.clear();

    }


    //TODO: IMPLEMENT BACKWARD SCANNING: Backwards scan requires fetching the latest version and adding to the map
    //backward here means that the version will be fetched from higher to lower
    //no guarantees of the order of the streams
    public IndexIterator indexedIterator(long stream) {
        return indexedIterator(Set.of(stream));
    }

    public IndexIterator indexedIterator(Map<Long, Integer> streams) {
        Map<Long, AtomicInteger> map = streams.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, kv -> new AtomicInteger(kv.getValue())));
        return new IndexIterator(Direction.FORWARD, map);
    }

    //TODO: IMPLEMENT BACKWARD SCANNING: Backwards scan requires fetching the latest version and adding to the map
    //backward here means that the version will be fetched from higher to lower
    //no guarantees of the order of the streams
    public IndexIterator indexedIterator(Set<Long> streams) {
        List<Long> streamList = new ArrayList<>(streams);
        Map<Long, AtomicInteger> map = streamList.stream().collect(Collectors.toMap(stream -> stream, r -> new AtomicInteger(NO_VERSION)));
        return new IndexIterator(Direction.FORWARD, map);
    }

    public Optional<IndexEntry> get(long stream, int version) {
        Optional<IndexEntry> fromMemory = memIndex.get(stream, version);
        if (fromMemory.isPresent()) {
            return fromMemory;
        }
        return diskIndex.get(stream, version);
    }

    public FlushInfo flush() {
        logger.info("Writing index to disk");

        long start = System.currentTimeMillis();
        diskIndex.writeToDisk(memIndex);
        long timeTaken = System.currentTimeMillis() - start;
        logger.info("Index write took {}ms", timeTaken);

        memIndex.close();
        memIndex = new MemIndex();

        return new FlushInfo(memIndex.size(), timeTaken);
    }

    public void compact() {
        diskIndex.compact();
    }

    public void truncate(long stream, int version) {
        if (version <= NO_VERSION) {
            throw new IllegalArgumentException("Version must be greater or equals zero");
        }
        int streamVersion = version(stream);
        if (streamVersion > version) {
            throw new IllegalArgumentException("Truncate version: " + version + " must be less or equals stream version: " + streamVersion);
        }

        memIndex.truncate(stream, version);
        diskIndex.truncate(stream, version);
    }

    public void delete(long stream) {
        int version = version(stream);
        if (version == NO_VERSION) {
            return; // no exception needed, Streams should just mark as deleted
        }

        memIndex.delete(stream);
        diskIndex.delete(stream);
    }


    public class IndexIterator implements LogIterator<IndexEntry> {

        private final Map<Long, AtomicInteger> streams = new ConcurrentHashMap<>();
        private final Queue<IndexEntry> queue = new ConcurrentLinkedDeque<>();
        private final Queue<Long> streamReadPriority;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final Direction direction;

        private IndexIterator(Direction direction, Map<Long, AtomicInteger> streamVersions) {
            this.streams.putAll(streamVersions);
            //deterministic behaviour
            ArrayList<Long> streamHashes = new ArrayList<>(streamVersions.keySet());
            streamHashes.sort(Comparator.comparingLong(c -> c));
            this.streamReadPriority = new ArrayDeque<>(streamHashes);
            this.direction = direction;
        }

        private IndexEntry computeAndGet(IndexEntry ie) {
            if (ie == null) {
                return null;
            }
            AtomicInteger lastVersion = streams.get(ie.stream);
            int lastReadVersion = lastVersion.get();
            if (Direction.FORWARD.equals(direction) && lastReadVersion >= ie.version) {
                throw new IllegalStateException("Reading already processed version, last processed version: " + lastVersion + " read version: " + ie.version);
            }
            if (Direction.BACKWARD.equals(direction) && lastReadVersion <= ie.version) {
                throw new IllegalStateException("Reading already processed version, last processed version: " + lastVersion + " read version: " + ie.version);
            }
            int expected = Direction.FORWARD.equals(direction) ? lastReadVersion + 1 : lastReadVersion - 1;
            if (expected != ie.version) {
                throw new IllegalStateException("Next expected version: " + expected + " got: " + ie.version + ", stream " + ie.stream);
            }
            if (Direction.FORWARD.equals(direction)) {
                lastVersion.incrementAndGet();
            } else {
                lastVersion.decrementAndGet();
            }
            return ie;
        }

        private void tryFetch() {
            if (queue.isEmpty()) {
                Queue<Long> emptyStreams = new ArrayDeque<>();
                while (!streamReadPriority.isEmpty() && queue.isEmpty()) {
                    long stream = streamReadPriority.peek();
                    int lastProcessedVersion = streams.get(stream).get();
                    List<IndexEntry> indexEntries = fetchEntries(stream, lastProcessedVersion);
                    if (!indexEntries.isEmpty()) {
                        queue.addAll(indexEntries);
                        streamReadPriority.addAll(emptyStreams);
                    } else {
                        streamReadPriority.poll();
                        emptyStreams.offer(stream);
                    }
                }
                streamReadPriority.addAll(emptyStreams);
            }
        }

        private List<IndexEntry> fetchEntries(long stream, int lastProcessedVersion) {
            int nextVersion = Direction.FORWARD.equals(direction) ? lastProcessedVersion + 1 : lastProcessedVersion - 1;
            List<IndexEntry> fromDisk = diskIndex.getBlockEntries(stream, nextVersion);
            List<IndexEntry> filtered = filtering(stream, fromDisk);
            if (!filtered.isEmpty()) {
                return filtered;
            }
            List<IndexEntry> fromMemory = memIndex.allOf(stream);
            return filtering(stream, fromMemory);
        }

        private List<IndexEntry> filtering(long stream, List<IndexEntry> original) {
            if (original.isEmpty() || !streams.containsKey(stream)) {
                return Collections.emptyList();
            }
            int lastRead = streams.get(stream).get();
            return original.stream().filter(ie -> {
                if (ie.stream != stream) {
                    return false;
                }
                if (Direction.FORWARD.equals(direction)) {
                    return ie.version > lastRead;
                }
                return ie.version < lastRead;
            }).collect(Collectors.toList());
        }

        @Override
        public boolean hasNext() {
            if (queue.isEmpty()) {
                tryFetch();
            }
            return !queue.isEmpty();
        }

        @Override
        public IndexEntry next() {
            tryFetch();
            IndexEntry entry = queue.poll();
            return computeAndGet(entry);
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