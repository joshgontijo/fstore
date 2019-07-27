package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;

import java.io.File;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SSTables<K extends Comparable<K>, V> implements TreeFunctions<K, V> {

    private final LogAppender<Entry<K, V>> appender;
    private final Cache<String, Block> blockCache; //propagated to sstables

    public SSTables(File dir,
                    Serializer<K> keySerializer,
                    Serializer<V> valueSerializer,
                    String name,
                    int segmentSize,
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

        this.blockCache = blockCache;
        this.appender = LogAppender.builder(dir, EntrySerializer.of(maxAge, keySerializer, valueSerializer))
                .compactionStrategy(compactor)
                .name(name + "-sstable")
                .storageMode(storageMode)
                .segmentSize(segmentSize)
                .parallelCompaction()
                .flushMode(flushMode)
                .directBufferPool()
                .open(new SSTable.SSTableFactory<>(keySerializer, valueSerializer, blockFactory, codec, footerCodec, bloomNItems, bloomFPProb, blockSize, maxAge, blockCache));
    }

    public long write(Entry<K, V> entry) {
        return appender.append(entry);
    }

    @Override
    public Entry<K, V> get(K key) {
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

    public <T> T applyToSegments(Direction direction, Function<List<Log<Entry<K, V>>>, T> func) {
        return appender.applyToSegments(direction, func);
    }

    public void roll() {
        appender.roll();
    }

    public void close() {
        appender.close();
    }

    public LogIterator<Entry<K, V>> iterator(Direction direction) {
        return appender.iterator(direction);
    }

    public List<LogIterator<Entry<K, V>>> segmentsIterator(Direction direction) {
        return appender.applyToSegments(Direction.FORWARD, segments -> segments.stream()
                .filter(Log::readOnly)
                .map(seg -> seg.iterator(direction))
                .collect(Collectors.toList()));
    }

    public List<LogIterator<Entry<K, V>>> segmentsIterator(Direction direction, Range<K> range) {
        return appender.applyToSegments(Direction.FORWARD, segments -> segments.stream()
                .filter(Log::readOnly)
                .map(seg -> {
                    SSTable<K, V> sstable = (SSTable<K, V>) seg;
                    return sstable.iterator(direction, range);
                })
                .collect(Collectors.toList()));
    }

    public long size() {
        return appender.applyToSegments(Direction.FORWARD, segs -> segs.stream().mapToLong(Log::entries).sum());
    }

    public void compact() {
        appender.compact();
    }
}
