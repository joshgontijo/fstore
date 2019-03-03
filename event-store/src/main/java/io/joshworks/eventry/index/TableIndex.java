package io.joshworks.eventry.index;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.index.disk.IndexAppender;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;

public class TableIndex implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);
    public static final int DEFAULT_FLUSH_THRESHOLD = 1000000;
    public static final boolean DEFAULT_USE_COMPRESSION = false;
    public static final int DEFAULT_WRITE_QUEUE_SIZE = 5;
    private final int flushThreshold; //TODO externalize

    private final IndexAppender diskIndex;
    private final List<MemIndex> writeQueue;
    private final ExecutorService indexWriter = Executors.newSingleThreadExecutor(Threads.namedThreadFactory("index-writer"));
    private final AtomicBoolean closed = new AtomicBoolean();
    private final int maxWriteQueueSize;

    private final Set<IndexIterator> pollers = new HashSet<>();
    private final Consumer<FlushInfo> indexFlushListener;

    private MemIndex memIndex = new MemIndex();

    public TableIndex(File rootDirectory, Function<Long, StreamMetadata> streamSupplier, Consumer<FlushInfo> indexFlushListener) {
        this(rootDirectory, streamSupplier, indexFlushListener, DEFAULT_FLUSH_THRESHOLD, DEFAULT_WRITE_QUEUE_SIZE, DEFAULT_USE_COMPRESSION);
    }

    public TableIndex(File rootDirectory, Function<Long, StreamMetadata> streamSupplier, Consumer<FlushInfo> indexFlushListener, int flushThreshold, int maxWriteQueueSize, boolean useCompression) {
        this.maxWriteQueueSize = maxWriteQueueSize;
        this.diskIndex = new IndexAppender(rootDirectory, streamSupplier, flushThreshold, useCompression);
        this.flushThreshold = flushThreshold;
        this.indexFlushListener = indexFlushListener;
        this.writeQueue = new CopyOnWriteArrayList<>();
        this.indexWriter.execute(this::writeIndex);

    }

    public void add(StreamName stream, long position) {
        add(stream.hash(), stream.version(), position);
    }

    //returns true if flushed to disk
    public void add(long stream, int version, long position) {
        if (version <= IndexEntry.NO_VERSION) {
            throw new IllegalArgumentException("Version must be greater than or equals to zero");
        }
        if (position < 0) {
            throw new IllegalArgumentException("Position must be greater than zero");
        }
        IndexEntry entry = IndexEntry.of(stream, version, position);
        memIndex.add(entry);

        if (memIndex.size() >= flushThreshold) {
            flushAsync();
        }
    }

    public int version(long stream) {
        Iterator<MemIndex> it = memIndices(Direction.BACKWARD);
        while (it.hasNext()) {
            MemIndex index = it.next();
            int version = index.version(stream);
            if (version > IndexEntry.NO_VERSION) {
                return version;
            }
        }
        return diskIndex.version(stream);
    }


    public long size() {
        return diskIndex.entries() + memIndex.size();
    }

    public void close() {
//        this.flush(); //no need to flush, just reload from disk on startup
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
        Iterator<MemIndex> it = memIndices(Direction.BACKWARD);
        while (it.hasNext()) {
            MemIndex index = it.next();
            Optional<IndexEntry> fromMemory = index.get(stream, version);
            if (fromMemory.isPresent()) {
                return fromMemory;
            }
        }
        return diskIndex.get(stream, version);
    }

    private void writeIndex() {
        try {
            while (!closed.get()) {
                if (writeQueue.isEmpty()) {
                    Threads.sleep(200);
                    continue;
                }
                logger.info("Writing index to disk");

                long start = System.currentTimeMillis();
                MemIndex index = writeQueue.get(0);
                diskIndex.writeToDisk(index);
                writeQueue.remove(0);
                long timeTaken = System.currentTimeMillis() - start;
                logger.info("Index write took {}ms", timeTaken);
                indexFlushListener.accept(new FlushInfo(index.size(), timeTaken));
            }

        } catch (Exception e) {
            throw new IllegalStateException("Failed to poll for mem index entries to write", e);
        }
    }

    //Adds a job to the queue
    public void flushAsync() {
        try {
            logger.info("Adding mem index to write queue");
            while (writeQueue.size() > maxWriteQueueSize) {
                Threads.sleep(100);
            }
            writeQueue.add(memIndex);
            memIndex = new MemIndex();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add mem index to write queue");
        }
    }

    public void compact() {
        diskIndex.compact();
    }

    private Iterator<MemIndex> memIndices(Direction direction) {
        List<MemIndex> indices = new ArrayList<>(writeQueue);
        indices.add(memIndex);

        return Direction.FORWARD.equals(direction) ? indices.iterator() : Iterators.reversed(indices);

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
            LogIterator<MemIndex> writeQueueIt = Iterators.of(writeQueue);
            while (writeQueueIt.hasNext()) {
                MemIndex index = writeQueueIt.next();
                List<IndexEntry> memEntries = fromMem(index, stream, nextVersion);
                if (!memEntries.isEmpty()) {
                    return memEntries;
                }
            }
            return fromMem(memIndex, stream, nextVersion);
        }

        private List<IndexEntry> fromMem(MemIndex index, long stream, int nextVersion) {
            List<IndexEntry> fromMemory = index.indexedIterator(Direction.FORWARD, Range.of(stream, nextVersion)).stream().collect(Collectors.toList());
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
            indexWriter.shutdown();
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