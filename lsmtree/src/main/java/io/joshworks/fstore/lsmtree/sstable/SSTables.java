package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class SSTables<K extends Comparable<K>, V> {

    private final LogAppender<Entry<K, V>> appender;

    public SSTables(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold, String name) {
        this.appender = LogAppender.builder(dir, new EntrySerializer<>(keySerializer, valueSerializer))
                .compactionStrategy(new SSTableCompactor<>())
                .name(name + "-sstable")
                .flushMode(FlushMode.ON_ROLL)
                .open(new SSTableFactory<>(dir, keySerializer, valueSerializer, flushThreshold));
    }

    //TODO SSTABLE must guarantee that all data from memtable is stored in a single segment
    //it currently is size based, for this, the segment would have to be unbounded, and rolling, manual
    public long write(Entry<K, V> entry) {
        return appender.append(entry);
    }

    public V getByKey(K key) {
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<Entry<K, V>> segment : segments) {
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
        return appender.applyToSegments(Direction.FORWARD, segments -> segments.stream().map(seg -> seg.iterator(Direction.FORWARD)).collect(Collectors.toList()));
    }

    private static class SSTableFactory<K extends Comparable<K>, V> implements SegmentFactory<Entry<K, V>> {
        private final File directory;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        private final int flushThreshold;

        public SSTableFactory(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold) {
            this.directory = directory;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.flushThreshold = flushThreshold;
        }

        @Override
        public Log<Entry<K, V>> createOrOpen(Storage storage, Serializer<Entry<K, V>> serializer, IDataStream reader, String magic, Type type) {
            return new SSTable<>(storage, keySerializer, valueSerializer, reader, magic, type, directory, flushThreshold);
        }
    }
}
