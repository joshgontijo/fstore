package io.joshworks.fstore.lsm.sstable;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.Type;

import java.io.File;
import java.io.IOException;

public class SSTables<K extends Comparable<K>, V> {

    private final LogAppender<Entry<K, V>> appender;

    public SSTables(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold) {
        this.appender = LogAppender.builder(dir, new EntrySerializer<>(keySerializer, valueSerializer))
                .compactionStrategy(new SSTableCompactor<>())
                .open(new SSTableFactory<>(dir, keySerializer, valueSerializer, flushThreshold));
    }

    public void append(Entry<K, V> entry) {
        appender.append(entry);
    }

    public V getByKey(K key) {
        try (LogIterator<Log<Entry<K, V>>> segments = appender.segments(Direction.BACKWARD)) {
            while(segments.hasNext()) {
                SSTable<K, V> sstable = (SSTable<K, V>) segments.next();
                V found = sstable.get(key);
                if(found != null) {
                    return found;
                }
            }
            return null;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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