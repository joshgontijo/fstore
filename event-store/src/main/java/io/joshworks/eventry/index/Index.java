package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.codec.snappy.LZ4Codec;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.streams.StreamHasher;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.Expression;
import io.joshworks.fstore.lsmtree.sstable.SSTables;
import io.joshworks.fstore.serializer.Serializers;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;


public class Index implements Closeable {

    //stream + version + position + timestamp
    private static final int INDEX_ENTRY_BYTES = IndexKey.BYTES + Long.BYTES + Long.BYTES;

    private static final String NAME = "index";
    private final SSTables<IndexKey, Long> sstables;
    private final Cache<Long, Integer> versionCache;
    private final Function<Long, StreamMetadata> metadataSupplier;

    public Index(File rootDir, int flushThreshold, Cache<Long, Integer> versionCache, Function<Long, StreamMetadata> metadataSupplier) {
        this.versionCache = versionCache;
        this.metadataSupplier = metadataSupplier;
        this.sstables = new SSTables<>(
                new File(rootDir, NAME),
                new IndexKeySerializer(),
                Serializers.LONG,
                NAME,
                INDEX_ENTRY_BYTES * flushThreshold,
                flushThreshold,
                StorageMode.MMAP,
                FlushMode.MANUAL,
                Block.flenBlock(INDEX_ENTRY_BYTES),
                new IndexCompactor(metadataSupplier, this::version),
                SSTables.NO_MAX_AGE,
                new LZ4Codec(),
                flushThreshold * 2,
                0.01,
                Size.KB.ofInt(4),
                Cache.lruCache(100, -1));
    }

    @Override
    public void close() {
        sstables.close();
    }

    public CompletableFuture<Void> add(long hash, int version, long position) {
        CompletableFuture<Void> flushTask = sstables.add(Entry.add(new IndexKey(hash, version), position));
        versionCache.add(hash, version);
        return flushTask;
    }

    public long size() {
        return sstables.size();
    }

    public void compact() {
        sstables.compact();
    }

    public Optional<IndexEntry> get(long stream, int version) {
        Entry<IndexKey, Long> entry = sstables.get(IndexKey.event(stream, version));
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
        Entry<IndexKey, Long> found = sstables.find(maxStreamVersion, Expression.FLOOR, entry -> matchStream(stream, entry));
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
        FixedIndexIterator iterator = new FixedIndexIterator(sstables, Direction.FORWARD, eventMap);
        return new IndexFilter(metadataSupplier, this::version, iterator);
    }

    public IndexIterator iterator(EventMap eventMap, Set<String> streamPatterns) {
        IndexPrefixIndexIterator iterator = new IndexPrefixIndexIterator(sstables, Direction.FORWARD, eventMap, streamPatterns);
        return new IndexFilter(metadataSupplier, this::version, iterator);
    }
}
