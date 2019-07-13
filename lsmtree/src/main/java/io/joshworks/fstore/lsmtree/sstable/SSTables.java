package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.block.BlockFactory;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class SSTables<K extends Comparable<K>, V> {

    private final LogAppender<Entry<K, V>> appender;

    public SSTables(File dir,
                    Serializer<K> keySerializer,
                    Serializer<V> valueSerializer,
                    String name,
                    StorageMode storageMode,
                    FlushMode flushMode,
                    BlockFactory blockFactory,
                    Codec codec,
                    long bloomNItems,
                    double bloomFPProb,
                    int blockSize,
                    int blockCacheSize,
                    int blockCacheMaxAge) {

        this.appender = LogAppender.builder(dir, new EntrySerializer<>(keySerializer, valueSerializer))
                .compactionStrategy(new SSTableCompactor<>())
                .name(name + "-sstable")
                .storageMode(storageMode)
                .flushMode(flushMode)
                .open(new SSTable.SSTableFactory<>(keySerializer, valueSerializer, blockFactory, codec, bloomNItems, bloomFPProb, blockSize, blockCacheSize, blockCacheMaxAge));
    }

    public long write(Entry<K, V> entry) {
        return appender.append(entry);
    }

    public V getByKey(K key) {
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

    public void roll() {
        appender.roll();
    }

    public void close() {
        appender.close();
    }

    public LogIterator<Entry<K, V>> iterator(Direction direction) {
        return appender.iterator(direction);
    }

    public List<LogIterator<Entry<K, V>>> segmentsIterator() {
        return appender.applyToSegments(Direction.FORWARD, segments -> segments.stream()
                .filter(Log::readOnly)
                .map(seg -> seg.iterator(Direction.FORWARD))
                .collect(Collectors.toList()));
    }

}
