package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.index.cache.Cache;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;

import java.io.File;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SSTables<K extends Comparable<K>, V> {

    private final LogAppender<Entry<K, V>> appender;
    private final Cache<String, Block> blockCache;

    public SSTables(File dir,
                    Serializer<K> keySerializer,
                    Serializer<V> valueSerializer,
                    String name,
                    int segmentSize,
                    StorageMode storageMode,
                    FlushMode flushMode,
                    BlockFactory blockFactory,
                    Codec codec,
                    long bloomNItems,
                    double bloomFPProb,
                    int blockSize,
                    int blockCacheSize,
                    int blockCacheMaxAge) {

        this.blockCache = Cache.create(blockCacheSize, blockCacheMaxAge);
        this.appender = LogAppender.builder(dir, new EntrySerializer<>(keySerializer, valueSerializer))
                .compactionStrategy(new SSTableCompactor<>())
                .name(name + "-sstable")
                .storageMode(storageMode)
                .segmentSize(segmentSize)
                .enableParallelCompaction()
                .flushMode(flushMode)
                .open(new SSTable.SSTableFactory<>(keySerializer, valueSerializer, blockFactory, codec, bloomNItems, bloomFPProb, blockSize, blockCache));
    }

    public long write(Entry<K, V> entry) {
        return appender.append(entry);
    }

    public V get(K key) {
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<Entry<K, V>> segment : segments) {
                if (!segment.readOnly()) {
                    continue;
                }
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                V found = sstable.get(key);
                if (found != null) {
                    return found;
                }
            }
            return null;
        });
    }

    public <T> T applyToSegments(Direction direction, Function<List<Log<Entry<K, V>>>, T> func) {
        return appender.applyToSegments(direction, func);
    }

    //returns the first floor occurrence of a given key
    public Entry<K, V> floor(K key) {
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<Entry<K, V>> segment : segments) {
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                if (!sstable.readOnly()) {
                    continue;
                }
                Entry<K, V> floorEntry = sstable.floor(key);
                if (floorEntry != null) {
                    return floorEntry;
                }
            }
            return null;
        });
    }

    //returns the first ceiling occurrence of a given key
    public Entry<K, V> ceiling(K key) {
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<Entry<K, V>> segment : segments) {
                if (!segment.readOnly()) {
                    continue;
                }
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                if (key.compareTo(sstable.firstKey()) > 0 && key.compareTo(sstable.lastKey()) <= 0) {
                    return sstable.ceiling(key);
                }
            }
            return null;
        });
    }

    public void evictBlockCache() {
        blockCache.clear();
    }

//    public Entry<K, V> ceiling(K key) {
//        Entry<K, V> ceiling = memTable.ceiling(key);
//    }
//
//    public Entry<K, V> higher(K key) {
//        Entry<K, V> higher = memTable.higher(key);
//    }
//
//    public Entry<K, V> lower(K key) {
//        Entry<K, V> lower = memTable.lower(key);
//    }


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
