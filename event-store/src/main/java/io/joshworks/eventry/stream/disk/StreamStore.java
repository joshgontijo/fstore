package io.joshworks.eventry.stream.disk;

import io.joshworks.eventry.LRUCache;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.StreamMetadataSerializer;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.serializer.Serializers;

import java.io.Closeable;
import java.io.File;
import java.util.Map;
import java.util.stream.Stream;

public class StreamStore implements Closeable {

    private static final String DIR = "streams";
    private final LsmTree<Long, StreamMetadata> store;
    private final Map<Long, StreamMetadata> cache;

    public StreamStore(File root, int cacheSize) {
        this.store = LsmTree.open(new File(root, DIR), Serializers.LONG, new StreamMetadataSerializer(), 10);
        this.cache = new LRUCache<>(cacheSize);
    }

    public void put(long stream, StreamMetadata metadata) {
        if(cache.containsKey(stream)) {
            throw new IllegalStateException("Stream '" + metadata.name + "' already exist");
        }
        StreamMetadata fromDisk = store.get(stream);
        if(fromDisk != null) {
            throw new IllegalStateException("Stream '" + metadata.name + "' already exist");
        }

        cache.put(stream, metadata);
        store.put(stream, metadata);
    }

    public StreamMetadata get(long stream) {
        return cache.computeIfAbsent(stream, store::get);
    }

    public StreamMetadata remove(long stream) {
        cache.remove(stream);
        StreamMetadata fromDisk = store.remove(stream);
        return fromDisk;
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
