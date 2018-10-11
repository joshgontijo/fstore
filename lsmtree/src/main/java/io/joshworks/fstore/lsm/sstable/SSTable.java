package io.joshworks.fstore.lsm.sstable;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.filter.BloomFilter;
import io.joshworks.fstore.core.filter.BloomFilterHasher;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.block.BlockSerializer;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.lsm.sstable.index.Index;
import io.joshworks.fstore.lsm.sstable.index.IndexEntry;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class SSTable<K extends Comparable<K>, V> extends BlockSegment<Entry<K, V>, VLenBlock<Entry<K, V>>> {

    private BloomFilter<K> filter;
    private final Index<K> index;
    private final Serializer<K> keySerializer;
    private final File directory;
    private static final int MAX_BLOCK_SIZE = Memory.PAGE_SIZE;

    private static final double FALSE_POSITIVE_PROB = 0.01;

    public SSTable(Storage storage,
                   Serializer<K> keySerializer,
                   Serializer<V> valueSerializer,
                   IDataStream reader,
                   String magic,
                   Type type,
                   File directory,
                   int numElements) {
        super(storage, new EntrySerializer<>(keySerializer, valueSerializer),
                new BlockSerializer<>(new EntrySerializer<>(keySerializer, valueSerializer), new SnappyCodec()),
                MAX_BLOCK_SIZE, reader, magic, type);
        this.keySerializer = keySerializer;

        this.index = new Index<>(directory, storage.name(), keySerializer, reader, magic);
        this.directory = directory;
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.Murmur64(keySerializer));
    }


    @Override
    public long append(Entry<K, V> data) {
        filter.add(data.key);
        long pos = super.append(data);
        index.add(data.key, pos);
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
        //TODO ideally, block position would be used, which required the block.add to return a position
        //within the block, which also requires a moderate refactoring on BlockAppender / LogAppender
        Block<Entry<K,V>> block = super.getBlock(indexEntry.position);

        List<Entry<K, V>> entries = block.entries();
        int idx = Collections.binarySearch(entries, Entry.keyOf(key));
        if(idx >= 0) {
            return entries.get(idx).value;
        }
        return null;
    }

    public synchronized void flush() {
        super.flush(); //flush super first, so writeBlock is called
        index.write();
        filter.write();
    }

    @Override
    public void delete() {
        super.delete();
        filter.delete();
        index.delete();
    }

    public void newBloomFilter(long numElements) {
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.Murmur64(keySerializer));
    }

    private boolean mightHaveEntry(K key) {
        return filter.contains(key);
    }


    @Override
    protected Block<Entry<K,V>> createBlock(Serializer<Entry<K, V>> serializer, int maxBlockSize) {
        return new VLenBlock<>(serializer, maxBlockSize);
    }

}
