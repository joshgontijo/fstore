package io.joshworks.eventry.index;

import io.joshworks.eventry.index.disk.IndexAppender;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableIndex implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);
    private final int flushThreshold;

    private final IndexAppender diskIndex;
    private final BlockingQueue<FlushTask> writeQueue;
    private final ExecutorService indexWriter = Executors.newSingleThreadExecutor(Threads.namedThreadFactory("index-writer"));
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Function<Long, StreamMetadata> metadataSupplier;

    private final Set<SingleIndexIterator> pollers = new HashSet<>();
    private final Consumer<FlushInfo> indexFlushListener;

    private MemIndex memIndex = new MemIndex();

    public TableIndex(File rootDirectory, Function<Long, StreamMetadata> metadataSupplier, Consumer<FlushInfo> indexFlushListener, int flushThreshold, int maxWriteQueueSize, Codec codec) {
        this.diskIndex = new IndexAppender(rootDirectory, metadataSupplier, flushThreshold, codec);
        this.metadataSupplier = metadataSupplier;
        this.flushThreshold = flushThreshold;
        this.indexFlushListener = indexFlushListener;
        this.writeQueue = new ArrayBlockingQueue<>(maxWriteQueueSize);
        this.indexWriter.execute(this::writeTask);
    }

    //returns true if flushed to disk
    public boolean add(long stream, int version, long position) {
        if (version <= EventRecord.NO_VERSION) {
            throw new IndexException("Version must be greater than or equals to zero");
        }
        if (position < 0) {
            throw new IndexException("Position must be greater than zero");
        }
        IndexEntry entry = IndexEntry.of(stream, version, position);
        memIndex.add(entry);

        return memIndex.size() >= flushThreshold;
    }

    public int version(long stream) {
        Iterator<MemIndex> it = memIndices(Direction.BACKWARD);
        while (it.hasNext()) {
            MemIndex index = it.next();
            int version = index.version(stream);
            if (version > EventRecord.NO_VERSION) {
                return version;
            }
        }
        return diskIndex.version(stream);
    }


    public long size() {
        return diskIndex.entries() + memIndex.size();
    }

    public int memSize() {
        return memIndex.size();
    }

    //TODO: IMPLEMENT BACKWARD SCANNING: Backwards scan requires fetching the latest version and adding to the map
    //backward here means that the version will be fetched from higher to lower
    //no guarantees of the order of the streams
    public IndexIterator indexedIterator(Checkpoint checkpoint) {
        validateCheckpoint(checkpoint);
        IndexIterator singleIndexIterator = new SingleIndexIterator(diskIndex, this::memIndices, Direction.FORWARD, checkpoint);
        return applyIteratorFilters(singleIndexIterator);
    }

    public IndexIterator indexedIterator(String prefix, Checkpoint checkpoint, Function<String, Set<Long>> streamFetcher) {
        validateCheckpoint(checkpoint);

        //contains all iterators that applies the checkpoints
        Function<String, Checkpoint> streamMatcher = (streamPrefix) -> createIteratorsForPrefix(streamPrefix, streamFetcher);
        IndexIterator singleIndexIterator = new StreamPrefixIndexIterator2(diskIndex, this::memIndices, Direction.FORWARD, checkpoint, prefix, streamMatcher);
        return applyIteratorFilters(singleIndexIterator);
    }

    private Checkpoint createIteratorsForPrefix(String prefix, Function<String, Set<Long>> streamMatcher) {
        Set<Long> found = streamMatcher.apply(prefix);
        return Checkpoint.of(found);
    }

    private void validateCheckpoint(Checkpoint checkpoint) {
        if (checkpoint == null || checkpoint.size() == 0) {
            throw new IllegalArgumentException("Checkpoint must be provided");
        }
    }

    private IndexIterator applyIteratorFilters(IndexIterator singleIndexIterator) {


        IndexIterator truncatedAware = new TruncatedAwareIterator(metadataSupplier, singleIndexIterator);
        return withMaxCountFilter(truncatedAware);
    }

    private IndexIterator withMaxCountFilter(IndexIterator iterator) {
        return new MaxCountFilteringIterator(metadataSupplier, this::version, iterator);
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

    private void writeTask() {
        try {
            while (!closed.get()) {
                if (writeQueue.isEmpty()) {
                    Threads.sleep(200);
                    continue;
                }
                while (!writeQueue.isEmpty()) {
                    FlushTask task = writeQueue.peek();
                    flushInternal(task.logPosition, task.memIndex);
                    writeQueue.poll();
                }

            }

        } catch (Exception e) {
            throw new IllegalStateException("Failed write index", e);
        }
    }

    private void flushInternal(long logPosition, MemIndex memIndex) {
        if (closed.get()) {
            logger.warn("Index closed, not flushing");
            return;
        }
        long start = System.currentTimeMillis();
        diskIndex.writeToDisk(memIndex);
        long timeTaken = System.currentTimeMillis() - start;
        logger.info("Index write took {}ms", timeTaken);
        indexFlushListener.accept(new FlushInfo(logPosition, memIndex.size(), timeTaken));
    }

    public synchronized void flush(long logPosition) {
        flushInternal(logPosition, memIndex);
        memIndex = new MemIndex();
    }

    //Adds a job to the queue
    public void flushAsync(long logPosition) {
        try {
            logger.info("Adding mem index to write queue");
            FlushTask flushTask = new FlushTask(logPosition, memIndex);
            if (!writeQueue.offer(flushTask)) {
                logger.warn("Full index write queue. Waiting for flush to complete");
                writeQueue.put(flushTask);
            }
            memIndex = new MemIndex();
        } catch (Exception e) {
            throw new IndexException("Failed to add mem index to write queue");
        }
    }

    public void compact() {
        diskIndex.compact();
    }

    private Iterator<MemIndex> memIndices(Direction direction) {
        List<MemIndex> indices = writeQueue.stream().map(ft -> ft.memIndex).collect(Collectors.toList());
        indices.add(memIndex);

        return Direction.FORWARD.equals(direction) ? indices.iterator() : Iterators.reversed(indices);

    }

    @Override
    public void close() {
//        this.flush(); //no need to flush, just reload from disk on startup
        if (closed.compareAndSet(false, true)) {
            Threads.awaitTerminationOf(indexWriter, 2, TimeUnit.SECONDS, () -> logger.info("Awaiting index writer to complete"));
            diskIndex.close();
            for (SingleIndexIterator poller : pollers) {
                IOUtils.closeQuietly(poller);
            }
            pollers.clear();
        }
    }

    public class FlushInfo {
        public final long logPosition;
        public final int entries;
        public final long timeTaken;

        private FlushInfo(long logPosition, int entries, long timeTaken) {
            this.logPosition = logPosition;
            this.entries = entries;
            this.timeTaken = timeTaken;
        }
    }

    private class FlushTask {
        private final long logPosition;
        private final MemIndex memIndex;

        private FlushTask(long logPosition, MemIndex memIndex) {
            this.logPosition = logPosition;
            this.memIndex = memIndex;
        }
    }
}