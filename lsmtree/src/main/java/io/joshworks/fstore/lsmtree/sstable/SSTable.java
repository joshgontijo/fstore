package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.index.Index;
import io.joshworks.fstore.index.IndexEntry;
import io.joshworks.fstore.index.SparseIndex;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class SSTable<K extends Comparable<K>, V> implements Log<Entry<K, V>> {


    private static final int MAX_BLOCK_SIZE = Memory.PAGE_SIZE;
    private final BlockSegment<Entry<K, V>> delegate;
    private final EntrySerializer<K, V> kvSerializer;
    private final Serializer<K> keySerializer;
    private Index<K> index;

    public SSTable(File file,
                   StorageMode mode,
                   long size,
                   Serializer<K> keySerializer,
                   Serializer<V> valueSerializer,
                   BufferPool bufferPool,
                   WriteMode writeMode,
                   double checksumProb,
                   int readPageSize) {

        this.keySerializer = keySerializer;
        this.kvSerializer = new EntrySerializer<>(keySerializer, valueSerializer);

        this.delegate = new BlockSegment<>(
                file,
                mode,
                size,
                bufferPool,
                writeMode,
                kvSerializer,
                VLenBlock.factory(),
                new SnappyCodec(),
                MAX_BLOCK_SIZE,
                checksumProb,
                readPageSize,
                (a, b) -> {
                },
                this::processBlockEntries);

        if (index == null) {
            index = SparseIndex.builder(keySerializer, delegate.footerReader()).build();
        }
    }

    @Override
    public long append(Entry<K, V> data) {
        long pos = delegate.append(data);
        index.add(data.key, pos);
        return pos;
    }

    V get(K key) {
        IndexEntry<K> indexEntry = index.get(key);
        if (indexEntry == null) {
            return null;
        }

        List<Entry<K, V>> entries = delegate.readBlockEntries(indexEntry.position);
        int idx = Collections.binarySearch(entries, Entry.key(key));
        if (idx < 0) {
            return null;
        }
        return entries.get(idx).value;
    }

    private void processBlockEntries(long blockPos, Block block) {
        if (index == null) {
            index = SparseIndex.builder(keySerializer, delegate.footerReader()).build();
        }

        List<Entry<K, V>> entries = block.deserialize(kvSerializer);
        for (Entry<K, V> entry : entries) {
            index.add(entry.key, blockPos);
        }
    }

    public synchronized void flush() {
        delegate.flush();
    }

    @Override
    public void delete() {
        delegate.delete();
    }

    @Override
    public void roll(int level) {
        delegate.roll(level);
    }

    @Override
    public boolean readOnly() {
        return delegate.readOnly();
    }

    @Override
    public boolean closed() {
        return delegate.closed();
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
    public void trim() {
        delegate.trim();
    }

    @Override
    public long uncompressedSize() {
        return delegate.uncompressedSize();
    }

    @Override
    public Type type() {
        return delegate.type();
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
    public long fileSize() {
        return delegate.fileSize();
    }

    @Override
    public long logSize() {
        return delegate.logSize();
    }

    @Override
    public long remaining() {
        return delegate.remaining();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public SegmentIterator<Entry<K, V>> iterator(long position, Direction direction) {
        return delegate.iterator(position, direction);
    }

    @Override
    public SegmentIterator<Entry<K, V>> iterator(Direction direction) {
        return delegate.iterator(direction);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
