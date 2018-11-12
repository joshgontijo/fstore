package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.filter.BloomFilter;
import io.joshworks.fstore.core.filter.BloomFilterHasher;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.LogPoller;
import io.joshworks.fstore.log.segment.TimeoutReader;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.lsmtree.sstable.index.Index;
import io.joshworks.fstore.lsmtree.sstable.index.IndexEntry;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class SSTable<K extends Comparable<K>, V> implements Log<Entry<K, V>> {

    private static final int MAX_BLOCK_SIZE = Memory.PAGE_SIZE;
    private static final double FALSE_POSITIVE_PROB = 0.01;

    private BloomFilter<K> filter;
    private final Index<K> index;
    private final Serializer<K> keySerializer;
    private final File directory;
    private final BlockSegment<Entry<K, V>> delegate;
    private final Map<K, Long> cache = new HashMap<>();

    public SSTable(Storage storage,
                   Serializer<K> keySerializer,
                   Serializer<V> valueSerializer,
                   IDataStream dataStream,
                   String magic,
                   Type type,
                   File directory,
                   int numElements) {

        this.delegate = new BlockSegment<>(
                storage,
                dataStream,
                magic,
                type,
                new EntrySerializer<>(keySerializer, valueSerializer),
                VLenBlock.factory(),
                new SnappyCodec(),
                MAX_BLOCK_SIZE);

        this.keySerializer = keySerializer;
        this.index = new Index<>(directory, storage.name(), keySerializer, dataStream, magic);
        this.directory = directory;
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.murmur64(keySerializer));
    }

    @Override
    public long append(Entry<K, V> data) {
        long pos = delegate.add(data);
        filter.add(data.key);
        index.add(data.key, pos);
        return pos;
    }

    V get(K key) {
        if (!mightHaveEntry(key)) {
            return null;
        }

        Long pos = cache.get(key);
        if(pos == null) {
            IndexEntry indexEntry = index.get(key);
            if (indexEntry == null) {
                return null;
            }
            pos = indexEntry.position;
            cache.put(key, pos);
        }

        Block<Entry<K, V>> foundBlock = delegate.get(pos);
        List<Entry<K, V>> entries = foundBlock.entries();
        int idx = Collections.binarySearch(entries, Entry.keyOf(key));
        if (idx < 0) {
            return null;
        }
        return entries.get(idx).value;
    }

    public void writeBlock() {
        delegate.writeBlock();
    }

    public synchronized void flush() {
        delegate.flush();
        index.write();
        filter.write();
    }

    @Override
    public SegmentState rebuildState(long lastKnownPosition) {
        return delegate.rebuildState(lastKnownPosition);
    }

    @Override
    public void delete() {
        delegate.delete();
        filter.delete();
        index.delete();
    }

    @Override
    public void roll(int level) {
        delegate.roll(level);
        index.roll();
    }


    @Override
    public boolean readOnly() {
        return delegate.readOnly();
    }

    @Override
    public long entries() {
        return delegate.entries();
    }

    @Override
    public int level() {
        return delegate.level();
    }

    @Override
    public long created() {
        return delegate.created();
    }

    @Override
    public Type type() {
        return delegate.type();
    }

    public void newBloomFilter(long numElements) {
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.murmur64(keySerializer));
    }

    private boolean mightHaveEntry(K key) {
        return filter.contains(key);
    }

    @Override
    public long position() {
        return delegate.position();
    }


    @Override
    public Entry<K, V> get(long position) {
        throw new UnsupportedOperationException("Operation is not supported on SSTables");
    }

    @Override
    public LogPoller<Entry<K, V>> poller(long position) {
        return delegate.entryPoller(position);
    }

    @Override
    public LogPoller<Entry<K, V>> poller() {
        return delegate.entryPoller();
    }


    @Override
    public long fileSize() {
        return delegate.fileSize();
    }

    @Override
    public long logicalSize() {
        return delegate.fileSize();
    }

    @Override
    public Set<TimeoutReader> readers() {
        return delegate.readers();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Stream<Entry<K, V>> stream(Direction direction) {
        return delegate.streamEntries(direction);
    }

    @Override
    public LogIterator<Entry<K, V>> iterator(long position, Direction direction) {
        return delegate.entryIterator(position, direction);
    }

    @Override
    public LogIterator<Entry<K, V>> iterator(Direction direction) {
        return delegate.entryIterator(direction);
    }

    @Override
    public void close() {
        delegate.close();
        index.close();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
