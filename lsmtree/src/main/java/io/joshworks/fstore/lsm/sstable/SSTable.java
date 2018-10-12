package io.joshworks.fstore.lsm.sstable;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.filter.BloomFilter;
import io.joshworks.fstore.core.filter.BloomFilterHasher;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.lsm.sstable.index.Index;
import io.joshworks.fstore.lsm.sstable.index.IndexEntry;

import java.io.File;

public class SSTable<K extends Comparable<K>, V> extends BlockSegment<Entry<K, V>> {

    private BloomFilter<K> filter;
    private final Index<K> index;
    private final Serializer<K> keySerializer;
    private final File directory;
    private static final int MAX_BLOCK_SIZE = Memory.PAGE_SIZE;

    private static final double FALSE_POSITIVE_PROB = 0.01;

    public SSTable(Storage storage,
                   Serializer<K> keySerializer,
                   Serializer<V> valueSerializer,
                   IDataStream dataStream,
                   String magic,
                   Type type,
                   BlockFactory<Entry<K, V>> blockFactory,
                   File directory,
                   int numElements) {
        super(storage, new EntrySerializer<>(keySerializer, valueSerializer), dataStream, magic, type, blockFactory, new SnappyCodec(), MAX_BLOCK_SIZE);
        this.keySerializer = keySerializer;

        this.index = new Index<>(directory, storage.name(), keySerializer, dataStream, magic);
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
        Entry<K, V> entry = super.get(indexEntry.position);
        return entry == null ? null : entry.value;
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


}
