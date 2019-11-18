package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.log.EntryAdded;
import io.joshworks.fstore.lsmtree.log.EntryDeleted;
import io.joshworks.fstore.lsmtree.log.LogRecord;
import io.joshworks.fstore.lsmtree.log.TransactionLog;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;
import io.joshworks.fstore.lsmtree.sstable.Expression;
import io.joshworks.fstore.lsmtree.sstable.SSTables;

import java.io.Closeable;
import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class LsmTree<K extends Comparable<K>, V> implements Closeable {

    private final SSTables<K, V> sstables;
    private final TransactionLog<K, V> log;

    LsmTree(Builder<K, V> builder) {
        this.sstables = createSSTable(builder);
        this.log = createTransactionLog(builder);
        this.log.restore(this::restore);
        this.sstables.flushSync();
    }

    public static <K extends Comparable<K>, V> Builder<K, V> builder(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new Builder<>(directory, keySerializer, valueSerializer);
    }

    private SSTables<K, V> createSSTable(Builder<K, V> builder) {
        return new SSTables<>(
                builder.directory,
                builder.keySerializer,
                builder.valueSerializer,
                builder.name,
                builder.segmentSize,
                builder.flushThreshold,
                builder.maxEntrySize,
                builder.sstableStorageMode,
                builder.ssTableFlushMode,
                builder.sstableCompactor,
                builder.maxAgeSeconds,
                builder.codec,
                builder.flushQueueSize,
                builder.parallelCompaction,
                builder.sstableCompactionThreshold,
                builder.directBufferPool,
                builder.bloomFPProb,
                builder.blockSize,
                builder.blockCache);
    }

    private TransactionLog<K, V> createTransactionLog(Builder<K, V> builder) {
        return new TransactionLog<>(
                builder.directory,
                builder.keySerializer,
                builder.valueSerializer,
                builder.tlogSize,
                builder.tlogCompactionThreshold,
                builder.name,
                builder.tlogStorageMode);
    }

    public void put(K key, V value) {
        requireNonNull(key, "Key must be provided");
        requireNonNull(value, "Value must be provided");
        log.append(LogRecord.add(key, value));

        Entry<K, V> entry = Entry.add(key, value);
        CompletableFuture<Void> flushTask = sstables.add(entry);
        if (flushTask != null) {
            String token = log.markFlushing();
            flushTask.thenRun(() -> log.markFlushed(token));
        }
    }

    public V get(K key) {
        Entry<K, V> entry = getEntry(key);
        return entry == null ? null : entry.value;
    }

    private Entry<K, V> getEntry(K key) {
        requireNonNull(key, "Key must be provided");
        return sstables.get(key);
    }

    public void remove(K key) {
        requireNonNull(key, "Key must be provided");
        log.append(LogRecord.delete(key));
        sstables.add(Entry.delete(key));
    }

    public void flush() {
        sstables.flushSync();
    }

    /**
     * Search for non deletion entries in all SSTables that matches the given {@link Predicate<Entry>}
     * SSTables are scanned from newest to oldest, including MemTable
     *
     * @param key        The key to look for
     * @param expression The expression function to apply
     * @param matcher    The matcher, used to filter entries that matches the expression
     * @return The list of found entries
     */
    public List<Entry<K, V>> findAll(K key, Expression expression, Predicate<Entry<K, V>> matcher) {
        return sstables.findAll(key, expression, matcher);
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
        return sstables.find(key, expression, matcher);
    }

    public long size() {
        return sstables.size();
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction) {
        return sstables.iterator(direction);
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        return sstables.iterator(direction, range);
    }

    @Override
    public void close() {
        sstables.close();
        log.close();
    }

    private void restore(LogRecord record) {
        switch (record.type) {
            case ADD:
                EntryAdded<K, V> added = (EntryAdded<K, V>) record;
                sstables.add(Entry.of(record.timestamp, added.key, added.value));
                break;
            case DELETE:
                EntryDeleted<K> deleted = (EntryDeleted<K>) record;
                sstables.add(Entry.of(record.timestamp, deleted.key, null));
                break;
        }
    }

    public void compact() {
        sstables.compact();
    }

}
