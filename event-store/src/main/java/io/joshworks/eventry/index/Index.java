package io.joshworks.eventry.index;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.index.cache.Cache;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.Expression;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.serializer.Serializers;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.joshworks.eventry.log.EventRecord.NO_VERSION;

public class Index implements Closeable {

    private static final String NAME = "index";
    private final LsmTree<IndexKey, Long> lsmTree;
    private final Cache<Long, AtomicInteger> versionCache;

    public Index(File rootDir, int indexFlushThreshold, Codec codec, int versionCacheSize, int versionCacheMaxAge) {
        versionCache = Cache.create(versionCacheSize, versionCacheMaxAge);
        lsmTree = LsmTree.builder(new File(rootDir, NAME), new IndexKeySerializer(), Serializers.LONG)
                .disableTransactionLog()
                .flushThreshold(indexFlushThreshold)
                .sstableStorageMode(StorageMode.MMAP)
                .codec(codec)
                .name(NAME)
                .sstableBlockFactory(IndexBlock.factory())
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
        Long entryPos = lsmTree.get(new IndexKey(stream, version));
        return Optional.ofNullable(entryPos).map(pos -> IndexEntry.of(stream, version, pos));
    }

    public int version(String stream) {
        return version(StreamName.hash(stream));
    }

    /**
     * Perform a backward scan on SSTables until the first key matching the stream
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

    public StreamIterator iterator(Checkpoint checkpoint, Function<Long, StreamMetadata> metadataSupplier) {
        FixedStreamIterator iterator = new FixedStreamIterator(lsmTree, Direction.FORWARD, checkpoint);
        return withMaxCount(iterator, metadataSupplier);
    }

    public StreamIterator iterator(String streamPrefix, Checkpoint checkpoint, Function<Long, StreamMetadata> metadataSupplier) {
        StreamPrefixIndexIterator iterator = new StreamPrefixIndexIterator(lsmTree, Direction.FORWARD, checkpoint, streamPrefix);
        return withMaxCount(iterator, metadataSupplier);
    }

    private StreamIterator withMaxCount(StreamIterator iterator, Function<Long, StreamMetadata> metadataSupplier) {
        return new MaxCountFilteringIterator(metadataSupplier, this::version, iterator);
    }
}
