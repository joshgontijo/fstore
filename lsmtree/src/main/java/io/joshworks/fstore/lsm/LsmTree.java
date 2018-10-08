package io.joshworks.fstore.lsm;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.lsm.log.Record;
import io.joshworks.fstore.lsm.log.TransactionLog;
import io.joshworks.fstore.lsm.mem.MemTable;
import io.joshworks.fstore.lsm.sstable.Entry;
import io.joshworks.fstore.lsm.sstable.EntrySerializer;
import io.joshworks.fstore.lsm.sstable.SSTable;
import io.joshworks.fstore.lsm.sstable.SSTables;
import io.joshworks.fstore.lsm.sstable.merge.IndexCompactor;

import java.io.Closeable;
import java.io.File;

public class LsmTree<K extends Comparable<K>, V> implements Closeable {

    private final SSTables<K, V> sstables;
    private final TransactionLog<K, V> log;
    private final MemTable<K, V> memTable;

    private LsmTree(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold) {
        sstables = new SSTables<>(
                LogAppender.builder(dir, new EntrySerializer<>(keySerializer, valueSerializer)).compactionStrategy(new IndexCompactor<>()),
                new IndexSegmentFactory<>(dir, flushThreshold, keySerializer, valueSerializer));
        log = new TransactionLog<>(dir, keySerializer, valueSerializer);
        memTable = new MemTable<>(flushThreshold);
        log.restore(this::restore);
    }

    public static <K extends Comparable<K>, V> LsmTree<K, V> of(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold) {
        return new LsmTree<>(dir, keySerializer, valueSerializer, flushThreshold);
    }

    private void restore(Record<K, V> record) {
        if(EntryType.ADD.equals(record.type)) {
            memTable.add(record.key, record.value);
        }
        if(EntryType.DELETE.equals(record.type)) {
            memTable.delete(record.key);
        }
    }

    public void put(K key, V value) {
        log.append(Record.add(key, value));
        if(memTable.add(key, value)) {
            flushMemTable();
        }
    }

    public V get(K key) {
        V found = memTable.get(key);
        if(found != null) {
            return found;
        }

        return sstables.getByKey(key);
    }

    public V delete(K key) {
        V deleted = memTable.delete(key);
        if(deleted != null) {
            return deleted;
        }
        V found = get(key);
        if(found == null) {
            return null;
        }
        log.append(Record.delete(key));
        return found;
    }

    public LogIterator<Entry<K, V>> iterator(Direction direction) {
        return sstables.iterator(direction);
    }

    private void flushMemTable() {
        memTable.sorted().forEach(sstables::append);
        sstables.roll();
        memTable.clear();
        log.markFlushed();
    }

    @Override
    public void close() {
        sstables.close();
        log.close();
    }

    private static class IndexSegmentFactory<K extends Comparable<K>, V> implements SegmentFactory<Entry<K, V>, SSTable<K, V>> {

        private final File directory;
        private final int numElements;
        private Serializer<K> keySerializer;
        private Serializer<V> valueSerializer;

        private IndexSegmentFactory(File directory, int numElements, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.directory = directory;
            this.numElements = numElements;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public SSTable<K, V> createOrOpen(Storage storage, Serializer<Entry<K, V>> serializer, IDataStream reader, String magic, Type type) {
            return new SSTable<>(storage, keySerializer, valueSerializer, reader, magic, type, directory, numElements);
        }
    }
}
