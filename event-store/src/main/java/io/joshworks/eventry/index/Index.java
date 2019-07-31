package io.joshworks.eventry.index;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.utils.Memory;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.Expression;
import io.joshworks.fstore.serializer.Serializers;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.joshworks.eventry.StreamName.NO_VERSION;


public class Index implements Closeable {

    //stream + version + position + timestamp
    private static final int INDEX_ENTRY_BYTES = IndexKey.BYTES + Long.BYTES + Long.BYTES;

    private static final String NAME = "index";
    private final LsmTree<IndexKey, Long> lsmTree;
    private final Cache<Long, AtomicInteger> versionCache;
    private final Function<Long, StreamMetadata> metadataSupplier;

    public Index(File rootDir, int indexFlushThreshold, Cache<Long, AtomicInteger> versionCache, Function<Long, StreamMetadata> metadataSupplier) {
        this.versionCache = versionCache;
        this.metadataSupplier = metadataSupplier;
        this.lsmTree = LsmTree.builder(new File(rootDir, NAME), new IndexKeySerializer(), Serializers.LONG)
                .disableTransactionLog()
                .flushThreshold(indexFlushThreshold)
                .sstableStorageMode(StorageMode.MMAP)
                .blockFactory(Block.flenBlock(INDEX_ENTRY_BYTES))
                .codec(new SnappyCodec())
                .blockSize(Memory.PAGE_SIZE * 2)
                .flushOnClose(false)
                .maxAge(Long.MAX_VALUE)
                .segmentSize(INDEX_ENTRY_BYTES * indexFlushThreshold)
                .sstableCompactor(new IndexCompactor(metadataSupplier, this::version))
                .name(NAME)
                .open();
    }

    @Override
    public void close() {
        lsmTree.close();
    }

    public boolean add(long hash, int version, long position) {
        updateVersionIfCached(hash, version);
        return lsmTree.put(new IndexKey(hash, version), position);
    }

    public long size() {
        return lsmTree.size();
    }

    public void compact() {
        lsmTree.compact();
    }

    public Optional<IndexEntry> get(long stream, int version) {
        Entry<IndexKey, Long> entry = lsmTree.getEntry(IndexKey.event(stream, version));
        return Optional.ofNullable(entry).map(pos -> IndexEntry.of(stream, version, entry.value, entry.timestamp));
    }

    public int version(String stream) {
        return version(StreamName.hash(stream));
    }

    /**
     * Performs a backward scan on SSTables until the first key matching the stream
     */
    public int version(long stream) {
        AtomicInteger cached = versionCache.get(stream);
        if (cached != null) {
            return cached.get();
        }
        IndexKey maxStreamVersion = IndexKey.allOf(stream).end();
        Entry<IndexKey, Long> found = lsmTree.find(maxStreamVersion, Expression.FLOOR, entry -> matchStream(stream, entry));
        if (found != null) {
            int fetched = found.key.version;
            versionCache.add(stream, new AtomicInteger(fetched));
            return fetched;
        }
        return NO_VERSION;
    }

    private static boolean matchStream(long stream, Entry<IndexKey, Long> entry) {
        return entry.key.stream == stream;
    }

    private void updateVersionIfCached(long hash, int version) {
        AtomicInteger cached = versionCache.get(hash);
        if (cached != null) {
            cached.set(version);
        }
    }

    public IndexIterator iterator(Checkpoint checkpoint) {
        FixedIndexIterator iterator = new FixedIndexIterator(lsmTree, Direction.FORWARD, checkpoint);
        return new IndexFilter(metadataSupplier, this::version, iterator);
    }

    public IndexIterator iterator(Checkpoint checkpoint, String... streamPatterns) {
        IndexPrefixIndexIterator iterator = new IndexPrefixIndexIterator(lsmTree, Direction.FORWARD, checkpoint, streamPatterns);
        return new IndexFilter(metadataSupplier, this::version, iterator);
    }
}
