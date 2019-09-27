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
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class SSTables<K extends Comparable<K>, V> implements TreeFunctions<K, V> {

    private MemTable<K, V> memTable;
    private final LogAppender<Entry<K, V>> appender;
    private final int flushThreshold;
    private final long maxAge;
    private final Cache<String, Block> blockCache; //propagated to sstables

    public static final long NO_MAX_AGE = -1;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final BlockingQueue<FlushTask> flushQueue = new ArrayBlockingQueue<>(3, false);

    private final Metrics metrics = new Metrics();
    private final String metricsKey;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final Object FLUSH_LOCK = new Object();

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
                    long maxAgeSeconds,
                    Codec codec,
                    long bloomNItems,
                    double bloomFPProb,
                    int blockSize,
                    Cache<String, Block> blockCache) {

        this.flushThreshold = flushThreshold;
        this.maxAge = maxAgeSeconds;
        this.blockCache = blockCache;
        this.memTable = new MemTable<>();
        this.executor.execute(flushTask());
        this.appender = LogAppender.builder(dir, EntrySerializer.of(maxAgeSeconds, keySerializer, valueSerializer))
                .compactionStrategy(compactor)
                .name(name + "-sstables")
                .storageMode(storageMode)
                .segmentSize(segmentSize)
                .parallelCompaction()
                .flushMode(flushMode)
                .directBufferPool()
                .open(new SSTable.SSTableFactory<>(keySerializer, valueSerializer, blockFactory, codec, bloomNItems, bloomFPProb, blockSize, maxAgeSeconds, blockCache));

        this.metricsKey = MetricRegistry.register(Map.of("type", "sstables", "name", name), () -> {
            metrics.set("blockCacheSize", blockCache.size());
            Metrics metrics = appender.metrics();
            return Metrics.merge(metrics, this.metrics);
        });

    }

    public CompletableFuture<Void> add(Entry<K, V> entry) {
        int size = memTable.add(entry);
        if (size >= flushThreshold) {
            return scheduleFlush(false);
        }
        return null;
    }

    private CompletableFuture<Void> scheduleFlush(boolean force) {
        synchronized (FLUSH_LOCK) {
            int size = memTable.size();
            if (!force && size < flushThreshold) {
                return null;
            }
            if (size == 0) {
                return null;
            }

            try {
                CompletableFuture<Void> completion = new CompletableFuture<>();
                flushQueue.put(new FlushTask(completion, memTable));
                memTable = new MemTable<>();
                return completion;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Failed to queue flush task", e);
            }
        }
    }

    private Runnable flushTask() {
        return () -> {
            while (!closed.get()) {
                FlushTask task = flushQueue.peek();
                if (task == null) {
                    Threads.sleep(500);
                    continue;
                }

                try {
                    long start = System.currentTimeMillis();

                    long inserted = task.memTable.writeTo(appender, maxAge);
                    flushQueue.poll();

                    long end = System.currentTimeMillis();
                    metrics.set("lastFlushTime", (end - start));
                    metrics.update("flushTime", (end - start));
                    metrics.update("flushed");
                    metrics.update("lastFlush", start);
                    metrics.update("flushedItems", inserted);
                } finally {
                    task.completionListener.complete(null);
                }
            }
        };
    }

    @Override
    public Entry<K, V> get(K key) {
        Entry<K, V> fromMem = memTable.get(key);
        if (fromMem != null) {
            metrics.update("memTableReads");
            return fromMem;
        }
        for (FlushTask flushTask : flushQueue) {
            MemTable<K, V> memTable = flushTask.memTable;
            fromMem = memTable.get(key);
            if (fromMem != null) {
                metrics.update("memTableReads");
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
                    metrics.update("sstableReads");
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
        for (FlushTask flushTask : flushQueue) {
            MemTable<K, V> memTable = flushTask.memTable;
            fromMem = expression.apply(key, memTable);
            if (matchEntry(matcher, fromMem)) {
                return fromMem;
            }
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
        metrics.update("findAll");
        List<Entry<K, V>> found = new ArrayList<>();
        Entry<K, V> memEntry = expression.apply(key, memTable);
        if (memEntry != null && matcher.test(memEntry)) {
            found.add(memEntry);
        }

        for (FlushTask flushTask : flushQueue) {
            MemTable<K, V> memTable = flushTask.memTable;
            memEntry = expression.apply(key, memTable);
            if (memEntry != null && matcher.test(memEntry)) {
                found.add(memEntry);
            }
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

            Entry<K, V> fromMem = expression.apply(key, memTable);
            if (fromMem != null) {
                if (key.equals(fromMem.key)) {
                    //short circuit on exact match when ceiling, floor or equals
                    if (Expression.CEILING.equals(expression) || Expression.FLOOR.equals(expression) || Expression.EQUALS.equals(expression)) {
                        return fromMem;
                    }
                }
                set.add(fromMem);
            }

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

    public CompletableFuture<Void> flush() {
        CompletableFuture<Void> completableFuture = scheduleFlush(true);
        if (completableFuture == null) {
            completableFuture = new CompletableFuture<>();
            completableFuture.complete(null);
        }
        return completableFuture;
    }

    public void flushSync() {
        try {
            CompletableFuture<Void> completion = scheduleFlush(true);
            if (completion != null) {
                completion.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public long memTableSize() {
        return memTable.size();
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
            MetricRegistry.remove(metricsKey);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            appender.close();
        }
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction) {
        synchronized (FLUSH_LOCK) {
            List<PeekingIterator<Entry<K, V>>> iterators = new ArrayList<>();

            //OLDEST -> NEWEST
            appender.acquireSegments(Direction.FORWARD, segments -> {
                segments.stream()
                        .filter(Log::readOnly)
                        .map(seg -> seg.iterator(direction))
                        .forEach(it -> iterators.add(Iterators.peekingIterator(it)));

                flushQueue.stream()
                        .map(t -> t.memTable)
                        .map(mem -> mem.iterator(direction))
                        .forEach(it -> iterators.add(Iterators.peekingIterator(it)));

                iterators.add(Iterators.peekingIterator(memTable.iterator(direction)));
            });

            return new SSTablesIterator<>(direction, iterators);
        }
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {

        synchronized (FLUSH_LOCK) {
            List<PeekingIterator<Entry<K, V>>> iterators = new ArrayList<>();

            //OLDEST -> NEWEST
            appender.acquireSegments(Direction.FORWARD, segments -> {
                segments.stream()
                        .filter(Log::readOnly)
                        .map(seg -> {
                            SSTable<K, V> sstable = (SSTable<K, V>) seg;
                            return sstable.iterator(direction, range);
                        })
                        .forEach(it -> iterators.add(Iterators.peekingIterator(it)));

                flushQueue.stream()
                        .map(t -> t.memTable)
                        .map(mem -> mem.iterator(direction, range))
                        .forEach(it -> iterators.add(Iterators.peekingIterator(it)));

                iterators.add(Iterators.peekingIterator(memTable.iterator(direction, range)));
            });

            return new SSTablesIterator<>(direction, iterators);
        }
    }

    public long size() {
        long sstablesSize = appender.applyToSegments(Direction.FORWARD, segs -> segs.stream().mapToLong(Log::entries).sum());
        return sstablesSize + memTableSize();
    }

    public void compact() {
        appender.compact();
    }

    private class FlushTask {

        final CompletableFuture<Void> completionListener;
        final MemTable<K, V> memTable;

        FlushTask(CompletableFuture<Void> completionListener, MemTable<K, V> memTable) {
            this.completionListener = completionListener;
            this.memTable = memTable;
        }
    }

}
