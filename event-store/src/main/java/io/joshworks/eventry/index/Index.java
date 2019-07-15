package io.joshworks.eventry.index;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;

public class Index implements Closeable {

    LsmTree<IndexKey, Long> lsmTree;

    public Index(File rootDir, Object fetchMetadata, int indexFlushThreshold, SnappyCodec snappyCodec) {
        LsmTree.builder(new File(rootDir, "index"), new IndexKeySerializer(), Serializers.LONG)
                .disableTransactionLog()
                .flushThreshold(indexFlushThreshold)
                .flushListener(onIndexFlush)
                .sstableStorageMode(StorageMode.MMAP)
                .sstableBlockFactory(IndexBlock.factory());
    }


    @Override
    public void close() {
        lsmTree.close();
    }

    public boolean add(long hash, int version, long position) {
        return lsmTree.put(new IndexKey(hash, version), position);
    }

    public long size() {
        return lsmTree.size();
    }

    public void compact() {
        lsmTree.compact();
    }

    public Optional<IndexEntry> get(long stream, int version) {
        Long entryPos = lsmTree.get(new IndexKey(stream, version));
        return Optional.ofNullable(entryPos).map(pos -> IndexEntry.of(stream, version, pos));
    }

    public IndexIterator iterator(Checkpoint checkpoint) {

    }

    public IndexIterator iterator(String streamPrefix, Checkpoint checkpoint, Object matchStreamHash) {
    }

    public int version(long stream) {

    }
}
