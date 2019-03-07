package io.joshworks.eventry.index;

import io.joshworks.eventry.index.disk.IndexAppender;
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

    private final Set<SingleIndexIterator> pollers = new HashSet<>();
    private final Consumer<FlushInfo> indexFlushListener;

    private MemIndex memIndex = new MemIndex();

    public TableIndex(File rootDirectory, Function<Long, StreamMetadata> streamSupplier, Consumer<FlushInfo> indexFlushListener, int flushThreshold, int maxWriteQueueSize, Codec codec) {
        this.diskIndex = new IndexAppender(rootDirectory, streamSupplier, flushThreshold, codec);
        this.flushThreshold = flushThreshold;
        this.indexFlushListener = indexFlushListener;
        this.writeQueue = new ArrayBlockingQueue<>(maxWriteQueueSize);
        this.indexWriter.execute(this::writeTask);
    }

    //returns true if flushed to disk
    public boolean add(long stream, int version, long position) {
        if (version <= IndexEntry.NO_VERSION) {
            throw new IllegalArgumentException("Version must be greater than or equals to zero");
        }
        if (position < 0) {
            throw new IllegalArgumentException("Position must be greater than zero");
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
            if (version > IndexEntry.NO_VERSION) {
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

    public IndexIterator indexedIterator(Checkpoint checkpoint) {
        if (checkpoint.size() > 1) {
            return indexedIterator(checkpoint, false);
        }
        long stream = checkpoint.keySet().iterator().next();
        int lastVersion = checkpoint.get(stream);
        return new SingleIndexIterator(diskIndex, this::memIndices, Direction.FORWARD, stream, lastVersion);
    }

    //TODO: IMPLEMENT BACKWARD SCANNING: Backwards scan requires fetching the latest version and adding to the map
    //backward here means that the version will be fetched from higher to lower
    //no guarantees of the order of the streams
    public IndexIterator indexedIterator(Checkpoint checkpoint, boolean ordered) {
        if (checkpoint.size() <= 1) {
            return indexedIterator(checkpoint);
        }

        int maxEntries = 1000000; //Useful only for ordered to prevent OOM. configurable ?
        int maxEntriesPerStream = ordered ? checkpoint.size() / maxEntries : -1;
        List<IndexIterator> iterators = checkpoint.entrySet()
                .stream()
                .map(kv -> new SingleIndexIterator(diskIndex, this::memIndices, Direction.FORWARD, kv.getKey(), kv.getValue(), maxEntriesPerStream))
                .collect(Collectors.toList());

        return new MultiStreamIndexIterator(iterators, ordered);
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
                flush();
            }

        } catch (Exception e) {
            throw new IllegalStateException("Failed write index", e);
        }
    }

    public synchronized void flush() {
        if (closed.get()) {
            logger.warn("Index closed, not flushing");
            return;
        }
        do {
            logger.info("Writing memindex to disk");
            long start = System.currentTimeMillis();
            FlushTask task = writeQueue.peek();
            if (task == null) {
                return;
            }
            diskIndex.writeToDisk(task.memIndex);
            writeQueue.poll();
            long timeTaken = System.currentTimeMillis() - start;
            logger.info("Index write took {}ms", timeTaken);
            indexFlushListener.accept(new FlushInfo(task.logPosition, task.memIndex.size(), timeTaken));
        } while (!writeQueue.isEmpty());
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
            throw new IllegalStateException("Failed to add mem index to write queue");
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