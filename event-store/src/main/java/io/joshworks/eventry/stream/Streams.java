package io.joshworks.eventry.stream;

import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.io.Closeable;
import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static io.joshworks.eventry.stream.StreamMetadata.STREAM_ACTIVE;
import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;
import static io.joshworks.fstore.es.shared.streams.StreamPattern.matchesPattern;
import static io.joshworks.fstore.es.shared.utils.StringUtils.requireNonBlank;

public class Streams implements Closeable {

    private static final String STORE_NAME = "streams";
    public final LsmTree<Long, StreamMetadata> store;
    private final Cache<Long, StreamMetadata> cache;

    public Streams(File root, int flushThreshold, Cache<Long, StreamMetadata> cache) {
        this.cache = cache;
        this.store = LsmTree.builder(new File(root, STORE_NAME), Serializers.LONG, JsonSerializer.of(StreamMetadata.class))
                .name(STORE_NAME)
                .flushThreshold(flushThreshold)
                .bloomFilter(0.01, flushThreshold)
                .transacationLogStorageMode(StorageMode.MMAP)
                .sstableStorageMode(StorageMode.MMAP)
                .open();
    }

    public StreamMetadata get(String stream) {
        return get(EventId.hash(stream));
    }

    public StreamMetadata get(long streamHash) {
        StreamMetadata cached = cache.get(streamHash);
        if (cached != null) {
            return cached;
        }
        StreamMetadata metadata = store.get(streamHash);
        if (metadata != null) {
            cache.add(streamHash, metadata);
        }
        return metadata;
    }

    public List<StreamMetadata> all() {
        return Iterators.stream(store.iterator(Direction.FORWARD)).map(e -> e.value).collect(Collectors.toList());
    }

    public StreamMetadata create(String stream) {
        return create(stream, NO_MAX_AGE, NO_MAX_COUNT);
    }

    public StreamMetadata create(String stream, long maxAge, int maxCount) {
        return create(stream, maxAge, maxCount, new HashMap<>(), new HashMap<>());
    }

    //return a metadata if existing, or create a new one using default values, invoking createdCallback on creation
    public StreamMetadata createIfAbsent(String stream, Consumer<StreamMetadata> createdCallback) {
        validateName(stream);
        long streamHash = EventId.hash(stream);
        StreamMetadata metadata = store.get(streamHash);
        if (metadata == null) {
            metadata = this.createInternal(stream, NO_MAX_AGE, NO_MAX_COUNT, new HashMap<>(), new HashMap<>(), streamHash);
            createdCallback.accept(metadata);
        }
        return metadata;
    }

    public StreamMetadata create(String stream, long maxAgeSec, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata) {
        validateName(stream);
        long hash = EventId.hash(stream);
        return createInternal(stream, maxAgeSec, maxCount, permissions, metadata, hash);
    }

    private synchronized StreamMetadata createInternal(String stream, long maxAgeSec, int maxCount, Map<String, Integer> acl, Map<String, String> metadata, long hash) {
        StreamMetadata streamMeta = new StreamMetadata(stream, hash, Instant.now().getEpochSecond(), maxAgeSec, maxCount, NO_TRUNCATE, acl, metadata, STREAM_ACTIVE);
        StreamMetadata fromDisk = store.get(hash);
        return fromDisk == null ? update(streamMeta) : fromDisk;
    }

    public void remove(long streamHash) {
        store.remove(streamHash);
    }

    Set<String> matchStreamName(Set<String> patterns) {
        if (patterns == null || patterns.isEmpty()) {
            return new HashSet<>();
        }
        return match(patterns)
                .map(stream -> stream.name)
                .collect(Collectors.toSet());
    }

    public Set<Long> matchStreamHash(Set<String> patterns) {
        if (patterns == null) {
            return new HashSet<>();
        }
        return match(patterns)
                .map(stream -> stream.hash)
                .collect(Collectors.toSet());
    }

    private Stream<StreamMetadata> match(Set<String> patterns) {
        return Iterators.stream(store.iterator(Direction.FORWARD))
                .map(e -> e.value)
                .filter(stream -> matchesPattern(stream.name, patterns));
    }

    @Override
    public void close() {
        store.close();
    }

    private void validateName(String streamName) {
        streamName = streamName.trim();
        requireNonBlank(streamName, "Stream name must be provided");
        if (streamName.contains(" ")) {
            throw new StreamException("Stream name must not contain whitespaces");
        }
        if (streamName.contains(EventId.STREAM_VERSION_SEPARATOR)) {
            throw new StreamException("Stream name must not contain " + EventId.STREAM_VERSION_SEPARATOR);
        }
    }

    public StreamMetadata truncate(StreamMetadata metadata, int currentVersion, int fromVersionInclusive) {
        if (currentVersion <= NO_VERSION) {
            throw new StreamException("Version must be greater or equals zero");
        }
        if (fromVersionInclusive > currentVersion) {
            throw new StreamException("Truncate version: " + fromVersionInclusive + " must be less or equals stream version: " + currentVersion);
        }

        StreamMetadata truncated = new StreamMetadata(metadata.name, metadata.hash, metadata.created, metadata.maxAgeSec, metadata.maxCount, fromVersionInclusive, metadata.acl, metadata.metadata, metadata.state);
        return update(truncated);
    }

    private synchronized StreamMetadata update(StreamMetadata metadata) {
        store.put(metadata.hash, metadata);
        cache.remove(metadata.hash);
        return metadata;
    }

}
