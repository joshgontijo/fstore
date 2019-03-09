package io.joshworks.eventry.stream;

import io.joshworks.eventry.LRUCache;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.serializer.Serializers;

import java.io.Closeable;
import java.io.File;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class StreamStore implements Closeable {

    private static final String DIR = "streams";
    private static final String STORE_NAME = "streams";
    public final LsmTree<Long, StreamMetadata> store;
    private final Map<Long, StreamMetadata> cache;

    StreamStore(File root, int cacheSize) {
        this.store = LsmTree.open(new File(root, DIR), Serializers.LONG, new StreamMetadataSerializer(), cacheSize, STORE_NAME); // doesnt need to be cacheSize
        this.cache = new LRUCache<>(cacheSize);
    }

    public void create(long stream, StreamMetadata metadata) {
        requireNonNull(metadata, "Metadata must be provided");
        if (cache.containsKey(stream)) {
            throw new StreamException("Stream '" + metadata.name + "' already exist");
        }
        StreamMetadata fromDisk = store.get(stream);
        if (fromDisk != null) {
            throw new StreamException("Stream '" + metadata.name + "' already exist");
        }

        cache.put(stream, metadata);
        store.put(stream, metadata);
    }

    public void update(StreamMetadata metadata) {
        requireNonNull(metadata, "Metadata must be provided");
        StreamMetadata fromDisk = store.get(metadata.hash);
        if (fromDisk == null) {
            throw new StreamException("Stream '" + metadata.name + "' doesn't exist");
        }

        cache.put(metadata.hash, metadata);
        store.put(metadata.hash, metadata);
    }

    public StreamMetadata get(long stream) {
        StreamMetadata metadata = cache.get(stream);
        if (metadata == null) {
            metadata = store.get(stream);
            if (metadata != null) {
                cache.put(stream, metadata);
            }
        }
        return metadata;
    }

    public StreamMetadata remove(long stream) {
        cache.remove(stream);
        return store.remove(stream);
    }

    public Stream<Entry<Long, StreamMetadata>> stream() {
        return store.stream();
    }

    @Override
    public void close() {
        store.close();
        cache.clear();
    }
}
