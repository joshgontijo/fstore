package io.joshworks.eventry.stream;

import io.joshworks.fstore.index.cache.Cache;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.serializer.Serializers;

import java.io.Closeable;
import java.io.File;

import static java.util.Objects.requireNonNull;

public class StreamStore implements Closeable {

    private static final String STORE_NAME = "streams";
    public final LsmTree<Long, StreamMetadata> store;
    private final Cache<Long, StreamMetadata> cache;

    StreamStore(File root, int cacheSize, int cacheMaxAge) {
        this.store = LsmTree.builder(new File(root, STORE_NAME), Serializers.LONG, new StreamMetadataSerializer())
                .flushThreshold(cacheSize)
                .name(STORE_NAME)
                .open();
        this.cache = Cache.create(cacheSize, cacheMaxAge);
    }

    public void create(long stream, StreamMetadata metadata) {
        requireNonNull(metadata, "Metadata must be provided");
        StreamMetadata cached = cache.get(stream);
        if (cached != null) {
            throw new StreamException("Stream '" + metadata.name + "' already exist");
        }
        StreamMetadata fromDisk = store.get(stream);
        if (fromDisk != null) {
            throw new StreamException("Stream '" + metadata.name + "' already exist");
        }

        cache.add(stream, metadata);
        store.put(stream, metadata);
    }

    public void update(StreamMetadata metadata) {
        requireNonNull(metadata, "Metadata must be provided");
        StreamMetadata fromDisk = store.get(metadata.hash);
        if (fromDisk == null) {
            throw new StreamException("Stream '" + metadata.name + "' doesn't exist");
        }

        cache.add(metadata.hash, metadata);
        store.put(metadata.hash, metadata);
    }

    public StreamMetadata get(long stream) {
        StreamMetadata metadata = cache.get(stream);
        if (metadata == null) {
            metadata = store.get(stream);
            if (metadata != null) {
                cache.add(stream, metadata);
            }
        }
        return metadata;
    }

    public boolean remove(long stream) {
        cache.remove(stream);
        return store.remove(stream);
    }

    public CloseableIterator<Entry<Long, StreamMetadata>> iterator(Direction direction) {
        return store.iterator(direction);
    }

    @Override
    public void close() {
        store.close();
        cache.clear();
    }
}
