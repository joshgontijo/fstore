package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.metrics.MetricRegistry;
import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class SSTables<K extends Comparable<K>, V> implements TreeFunctions<K, V> {

    private MemTable<K, V> memTable;
    private final LogAppender<Entry<K, V>> appender;
    private final int flushThreshold;
    private final long maxAge;
    private final Cache<String, Block> blockCache; //propagated to sstables

    private final AtomicBoolean closed = new AtomicBoolean();
    private final BlockingQueue<MemTable<K, V>> flushQueue = new ArrayBlockingQueue<>(3, false);

    private final Metrics metrics;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public SSTables(File dir,
                    Serializer<K> keySerializer,
                    Serializer<V> valueSerializer,
                    String name,
                    long segmentSize,
                    int flushThreshold,
                    StorageMode storageMode,
                    FlushMode flushMode,
                    BlockFactory blockFactory,
                    UniqueMergeCombiner<Entry<K, V>> compactor,
                    long maxAge,
                    Codec codec,
                    Codec footerCodec,
                    long bloomNItems,
                    double bloomFPProb,
                    int blockSize,
                    Cache<String, Block> blockCache) {

        this.flushThreshold = flushThreshold;
        this.maxAge = maxAge;
        this.blockCache = blockCache;
        this.memTable = new MemTable<>();
        name = name + "-sstables";
        executor.execute(flushTask());
        this.metrics = MetricRegistry.create(name);
        this.appender = LogAppender.builder(dir, EntrySerializer.of(maxAge, keySerializer, valueSerializer))
                .compactionStrategy(compactor)
                .name(name)
                .storageMode(storageMode)
                .segmentSize(segmentSize)
                .parallelCompaction()
                .flushMode(flushMode)
                .directBufferPool()
                .open(new SSTable.SSTableFactory<>(keySerializer, valueSerializer, blockFactory, codec, footerCodec, bloomNItems, bloomFPProb, blockSize, maxAge, blockCache));
    }

    public void add(Entry<K, V> entry) {
        int size = memTable.add(entry);
        if (size >= flushThreshold) {
            scheduleFlush(false);
        }
    }

    private synchronized boolean scheduleFlush(boolean force) {
        int size = memTable.size();
        if (size == 0) {
            return false;
        }
        if (!force && size < flushThreshold) {
            return false;
        }
        flushQueue.add(memTable);
        memTable = new MemTable<>();
        return true;
    }

    private Runnable flushTask() {
        return () -> {
            while (!closed.get()) {
                MemTable<K, V> polled = flushQueue.peek();
                if (polled == null) {
                    Threads.sleep(500);
                    continue;
                }

                long start = System.currentTimeMillis();
                long inserted = polled.writeTo(appender, maxAge);
                long end = System.currentTimeMillis();

                flushQueue.poll();
                if (inserted > 0) {
                    log.markFlushed();
                }


                metrics.set("lastFlushTime", (end - start));
                metrics.update("flushTime", (end - start));
                metrics.update("flushed");
                metrics.update("lastFlush", start);
                metrics.update("flushedItems", inserted);


            }
        };
    }

    @Override
    public Entry<K, V> get(K key) {
        Entry<K, V> fromMem = memTable.get(key);
        if (fromMem != null) {
            return fromMem;
        }
        for (MemTable<K, V> memTable : flushQueue) {
            fromMem = memTable.get(key);
            if (fromMem != null) {
                return fromMem;
            }
        }
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<Entry<K, V>> segment : segments) {
                if (!segment.readOnly()) {
                    continue;
                }
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                Entry<K, V> found = sstable.get(key);
                if (found != null) {
                    return found;
                }
            }
            return null;
        });
    }

    @Override
    public Entry<K, V> floor(K key) {
        return apply(key, Expression.FLOOR);
    }

    @Override
    public Entry<K, V> lower(K key) {
        return apply(key, Expression.LOWER);
    }

    @Override
    public Entry<K, V> ceiling(K key) {
        return apply(key, Expression.CEILING);
    }

    @Override
    public Entry<K, V> higher(K key) {
        return apply(key, Expression.HIGHER);
    }

    /**
     * Finds the newest, non deletion entry that matches the give {@link Predicate<Entry>}
     * SSTables are scanned from newest to oldest, including MemTable
     *
     * @param key        The key to look for
     * @param expression The expression function to apply
     * @param matcher    The matcher, used to filter entries that matches the expression
     * @return The first match, or null
     */
    public Entry<K, V> find(K key, Expression expression, Predicate<Entry<K, V>> matcher) {
        metrics.update("finds");
        Entry<K, V> fromMem = expression.apply(key, memTable);
        if (matchEntry(matcher, fromMem)) {
            return fromMem;
        }
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<Entry<K, V>> segment : segments) {
                if (!segment.readOnly()) {
                    continue;
                }
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                Entry<K, V> fromDisk = expression.apply(key, sstable);
                if (matchEntry(matcher, fromDisk)) {
                    return fromDisk;
                }
            }
            return null;
        });
    }

    /**
     * Search for non deletion entries in all SSTables that matches the given {@link Predicate <Entry>}
     * SSTables are scanned from newest to oldest, including MemTable
     *
     * @param key        The key to look for
     * @param expression The expression function to apply
     * @param matcher    The matcher, used to filter entries that matches the expression
     * @return The list of found entries
     */
    public List<Entry<K, V>> findAll(K key, Expression expression, Predicate<Entry<K, V>> matcher) {
        requireNonNull(key, "Key must be provided");
        List<Entry<K, V>> found = new ArrayList<>();
        Entry<K, V> memEntry = expression.apply(key, memTable);
        if (memEntry != null && matcher.test(memEntry)) {
            found.add(memEntry);
        }
        List<Entry<K, V>> fromDisk = appender.applyToSegments(Direction.BACKWARD, segments -> {
            List<Entry<K, V>> diskEntries = new ArrayList<>();
            for (Log<Entry<K, V>> segment : segments) {
                if (!segment.readOnly()) {
                    continue;
                }
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                Entry<K, V> diskEntry = expression.apply(key, sstable);
                if (diskEntry != null && matcher.test(diskEntry)) {
                    diskEntries.add(diskEntry);
                }
            }
            return diskEntries;
        });
        found.addAll(fromDisk);
        return found;
    }

    private Entry<K, V> apply(K key, Expression expression) {
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            TreeSet<Entry<K, V>> set = new TreeSet<>();
            for (Log<Entry<K, V>> segment : segments) {
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                if (!sstable.readOnly()) {
                    continue;
                }
                Entry<K, V> entry = expression.apply(key, sstable);
                if (entry != null) {
                    set.add(entry);
                    if (key.equals(entry.key)) {
                        //short circuit on exact match when ceiling, floor or equals
                        if (Expression.CEILING.equals(expression) || Expression.FLOOR.equals(expression) || Expression.EQUALS.equals(expression)) {
                            break;
                        }
                    }
                }
            }
            return expression.apply(key, set);
        });
    }

    private boolean matchEntry(Predicate<Entry<K, V>> matcher, Entry<K, V> entry) {
        return entry != null && !entry.deletion() && matcher.test(entry);
    }

    public void flush() {
        scheduleFlush(true);
    }

    public void close() {

        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            appender.close();
        }
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction) {
        List<CloseableIterator<Entry<K, V>>> sstableIterators = appender.applyToSegments(Direction.FORWARD, segments -> segments.stream()
                .filter(Log::readOnly)
                .map(seg -> seg.iterator(direction))
                .collect(Collectors.toList()));

        return new SSTablesIterator<>(sstableIterators, memTable.iterator(direction));

    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        List<CloseableIterator<Entry<K, V>>> sstableIterators = appender.applyToSegments(Direction.FORWARD, segments -> segments.stream()
                .filter(Log::readOnly)
                .map(seg -> {
                    SSTable<K, V> sstable = (SSTable<K, V>) seg;
                    return sstable.iterator(direction, range);
                })
                .collect(Collectors.toList()));

        return new SSTablesIterator<>(sstableIterators, memTable.iterator(direction, range));
    }

    public long size() {
        return appender.applyToSegments(Direction.FORWARD, segs -> segs.stream().mapToLong(Log::entries).sum());
    }

    public void compact() {
        appender.compact();
    }
}
