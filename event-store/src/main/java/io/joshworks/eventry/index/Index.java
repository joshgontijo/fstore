package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.serializer.Serializers;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.joshworks.eventry.log.EventRecord.NO_VERSION;

public class Index implements Closeable {

    public static final String DIR = "index";
    private final LsmTree<IndexKey, Long> lsmTree;

    public Index(File rootDir, int indexFlushThreshold, Codec codec) {
        lsmTree = LsmTree.builder(new File(rootDir, DIR), new IndexKeySerializer(), Serializers.LONG)
                .disableTransactionLog()
                .flushThreshold(indexFlushThreshold)
                .sstableStorageMode(StorageMode.MMAP)
                .codec(codec)
                .sstableBlockFactory(IndexBlock.factory())
                .open();
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

    public int version(long stream) {
        IndexKey maxStreamVersion = IndexKey.allOf(stream).end();
        Entry<IndexKey, Long> found = lsmTree.firstFloor(maxStreamVersion);
        return found == null ? NO_VERSION : found.key.version;
    }

    public IndexIterator iterator(Checkpoint checkpoint) {
        return new IndexIterator(lsmTree, Direction.FORWARD, checkpoint);
    }

    public IndexIterator iterator(String streamPrefix, Checkpoint checkpoint, Function<String, Set<Long>> streamMatcher) {
        Function<String, Checkpoint> checkpointMatcher = stream -> Checkpoint.of(streamMatcher.apply(stream));
        return new StreamPrefixIndexIterator(lsmTree, Direction.FORWARD, checkpoint, streamPrefix, checkpointMatcher);
    }
}
