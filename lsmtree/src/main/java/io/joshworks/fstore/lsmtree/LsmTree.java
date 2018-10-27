package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.lsmtree.log.Record;
import io.joshworks.fstore.lsmtree.log.TransactionLog;
import io.joshworks.fstore.lsmtree.mem.MemTable;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.SSTables;

import java.io.Closeable;
import java.io.File;

public class LsmTree<K extends Comparable<K>, V> implements Closeable {

    private final SSTables<K, V> sstables;
    private final TransactionLog<K, V> log;
    private final MemTable<K, V> memTable;

    private LsmTree(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold) {
        sstables = new SSTables<>(dir, keySerializer, valueSerializer, flushThreshold);
        log = new TransactionLog<>(dir, keySerializer, valueSerializer);
        memTable = new MemTable<>(flushThreshold);
        log.restore(this::restore);
    }

    public static <K extends Comparable<K>, V> LsmTree<K, V> of(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold) {
        return new LsmTree<>(dir, keySerializer, valueSerializer, flushThreshold);
    }

    private void restore(Record<K, V> record) {
        if (EntryType.ADD.equals(record.type)) {
            memTable.add(record.key, record.value);
        }
        if (EntryType.DELETE.equals(record.type)) {
            memTable.delete(record.key);
        }
    }

    public void put(K key, V value) {
        log.append(Record.add(key, value));
        if (memTable.add(key, value)) {
            flushMemTable();
            log.markFlushed();
        }
    }

    public V get(K key) {
        V found = memTable.get(key);
        if (found != null) {
            return found;
        }

        return sstables.getByKey(key);
    }

    public V delete(K key) {
        V deleted = memTable.delete(key);
        if (deleted != null) {
            return deleted;
        }
        V found = get(key);
        if (found == null) {
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
    }

    @Override
    public void close() {
        sstables.close();
        log.close();
    }

}
