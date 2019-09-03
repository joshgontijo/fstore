package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.streams.StreamHasher;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.Expression;
import io.joshworks.fstore.serializer.Serializers;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;


public class Index implements Closeable {

    //stream + version + position + timestamp
    private static final int INDEX_ENTRY_BYTES = IndexKey.BYTES + Long.BYTES + Long.BYTES;

    private static final String NAME = "index";
    private final LsmTree<IndexKey, Long> lsmTree;
    private final Cache<Long, Integer> versionCache;
    private final Function<Long, StreamMetadata> metadataSupplier;

    public Index(File rootDir, int indexFlushThreshold, Cache<Long, Integer> versionCache, Function<Long, StreamMetadata> metadataSupplier) {
        this.versionCache = versionCache;
        this.metadataSupplier = metadataSupplier;
        this.lsmTree = LsmTree.builder(new File(rootDir, NAME), new IndexKeySerializer(), Serializers.LONG)
                .disableTransactionLog()
                .flushThreshold(indexFlushThreshold)
                .sstableStorageMode(StorageMode.MMAP)
                .blockFactory(Block.flenBlock(INDEX_ENTRY_BYTES))
                .codec(new SnappyCodec())
                .blockSize(Memory.PAGE_SIZE)
                .flushOnClose(false)
                .blockCache(Cache.lruCache(100, 60))
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
        boolean put = lsmTree.put(new IndexKey(hash, version), position);
        versionCache.add(hash, version);
        return put;
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
        return version(StreamHasher.hash(stream));
    }

    /**
     * Performs a backward scan on SSTables until the first key matching the stream
     */
    public int version(long stream) {
        Integer cached = versionCache.get(stream);
        if (cached != null) {
            return cached;
        }
        IndexKey maxStreamVersion = IndexKey.allOf(stream).end();
        Entry<IndexKey, Long> found = lsmTree.find(maxStreamVersion, Expression.FLOOR, entry -> matchStream(stream, entry));
        if (found != null) {
            int fetched = found.key.version;
            versionCache.add(stream, fetched);
            return fetched;
        }
        return NO_VERSION;
    }

    private static boolean matchStream(long stream, Entry<IndexKey, Long> entry) {
        return entry.key.stream == stream;
    }


    public IndexIterator iterator(EventMap eventMap) {
        FixedIndexIterator iterator = new FixedIndexIterator(lsmTree, Direction.FORWARD, eventMap);
        return new IndexFilter(metadataSupplier, this::version, iterator);
    }

    public IndexIterator iterator(EventMap eventMap, Set<String> streamPatterns) {
        IndexPrefixIndexIterator iterator = new IndexPrefixIndexIterator(lsmTree, Direction.FORWARD, eventMap, streamPatterns);
        return new IndexFilter(metadataSupplier, this::version, iterator);
    }
}
