package io.joshworks.fstore.lsm.sstable;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.filter.BloomFilter;
import io.joshworks.fstore.core.filter.BloomFilterHasher;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Marker;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.BlockSerializer;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.lsm.sstable.index.Index;
import io.joshworks.fstore.lsm.sstable.index.IndexEntry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public class SSTable<K extends Comparable<K>, V> implements Log<Entry<K, V>> {

    private final Segment<Block<Entry<K, V>>> delegate;
    private BloomFilter<K> filter;
    private final Index<K> index;
    private final Serializer<K> keySerializer;
    private final File directory;
    private static final int MAX_BLOCK_SIZE = Memory.PAGE_SIZE;

    private Block<Entry<K, V>> block;

    private static final double FALSE_POSITIVE_PROB = 0.01;
    private final BlockFactory<Entry<K, V>> blockFactory;
    private final EntrySerializer<K, V> entrySerializer;

    public SSTable(Storage storage,
                   Serializer<K> keySerializer,
                   Serializer<V> valueSerializer,
                   IDataStream dataStream,
                   String magic,
                   Type type,

                   File directory,
                   int numElements) {
        this.entrySerializer = new EntrySerializer<>(keySerializer, valueSerializer);
        this.blockFactory = VLenBlock.factory();
        this.delegate = new Segment<>(storage, new BlockSerializer<>(new SnappyCodec(), entrySerializer, blockFactory), dataStream, magic, type);
        this.keySerializer = keySerializer;
        this.block = blockFactory.create(entrySerializer, MAX_BLOCK_SIZE);
        this.index = new Index<>(directory, storage.name(), keySerializer, dataStream, magic);
        this.directory = directory;
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.murmur64(keySerializer));
    }

    @Override
    public long append(Entry<K, V> data) {
        Objects.requireNonNull(data);
        Objects.requireNonNull(data.key);
        Objects.requireNonNull(data.value);

        long pos = delegate.position();
        if(block.add(data)) {
            writeBlock();
        }
        filter.add(data.key);
        index.add(data.key, pos);
        return pos;
    }

    private long writeBlock() {
        long pos = delegate.append(block);
        block = blockFactory.create(entrySerializer, MAX_BLOCK_SIZE);
        return pos;
    }

    public V get(K key) {
        if (!mightHaveEntry(key)) {
            return null;
        }
        IndexEntry indexEntry = index.get(key);
        if (indexEntry == null) {
            return null;
        }
        Block<Entry<K, V>> block = delegate.get(indexEntry.position);
        List<Entry<K, V>> entries = block.entries();
        int idx = Collections.binarySearch(entries, Entry.keyOf(key));
        if(idx >= 0) {
            return entries.get(idx).value;
        }
        return null;
    }

    public synchronized void flush() {
        delegate.flush(); //flush super first, so writeBlock is called
        index.write();
        filter.write();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Stream<Entry<K, V>> stream(Direction direction) {
        return null;
    }

    @Override
    public LogIterator<Entry<K, V>> iterator(long position, Direction direction) {
        return null;
    }

    @Override
    public LogIterator<Entry<K, V>> iterator(Direction direction) {
        return null;
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public Marker marker() {
        return null;
    }

    @Override
    public Entry<K, V> get(long position) {
        return null;
    }

    @Override
    public PollingSubscriber<Entry<K, V>> poller(long position) {
        return null;
    }

    @Override
    public PollingSubscriber<Entry<K, V>> poller() {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public Set<TimeoutReader> readers() {
        return null;
    }

    @Override
    public SegmentState rebuildState(long lastKnownPosition) {
        return null;
    }

    @Override
    public void delete() {
        delegate.delete();
        filter.delete();
        index.delete();
    }

    @Override
    public void roll(int level) {

    }

    @Override
    public void roll(int level, ByteBuffer footer) {

    }

    @Override
    public ByteBuffer readFooter() {
        return null;
    }

    @Override
    public boolean readOnly() {
        return false;
    }

    @Override
    public long entries() {
        return 0;
    }

    @Override
    public int level() {
        return 0;
    }

    @Override
    public long created() {
        return 0;
    }

    public void newBloomFilter(long numElements) {
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.murmur64(keySerializer));
    }

    private boolean mightHaveEntry(K key) {
        return filter.contains(key);
    }


    @Override
    public void close() throws IOException {

    }
}
