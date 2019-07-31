package io.joshworks.eventry.stream;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;

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

import static io.joshworks.eventry.StreamName.NO_VERSION;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static io.joshworks.eventry.stream.StreamMetadata.STREAM_ACTIVE;

public class Streams implements Closeable {

    private static final String WILDCARD = "*";
    private static final String STORE_NAME = "streams";
    public final LsmTree<Long, StreamMetadata> store;
    private final Cache<Long, StreamMetadata> cache;

    public Streams(File root, int flushThreshold, Cache<Long, StreamMetadata> cache) {
        this.cache = cache;
        this.store = LsmTree.builder(new File(root, STORE_NAME), Serializers.LONG, new StreamMetadataSerializer())
                .name(STORE_NAME)
                .flushThreshold(flushThreshold)
                .bloomFilter(0.01, flushThreshold)
                .transacationLogStorageMode(StorageMode.MMAP)
                .sstableStorageMode(StorageMode.MMAP)
                .open();
    }

    public StreamMetadata get(String stream) {
        return get(StreamName.hash(stream));
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
        long streamHash = StreamName.hash(stream);
        StreamMetadata metadata = store.get(streamHash);
        if (metadata == null) {
            metadata = this.createInternal(stream, NO_MAX_AGE, NO_MAX_COUNT, new HashMap<>(), new HashMap<>(), streamHash);
            createdCallback.accept(metadata);
        }
        return metadata;
    }

    public StreamMetadata create(String stream, long maxAgeSec, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata) {
        validateName(stream);
        long hash = StreamName.hash(stream);
        return createInternal(stream, maxAgeSec, maxCount, permissions, metadata, hash);
    }

    //must not hold the lock, since
    private StreamMetadata createInternal(String stream, long maxAgeSec, int maxCount, Map<String, Integer> acl, Map<String, String> metadata, long hash) {
        StreamMetadata streamMeta = new StreamMetadata(stream, hash, Instant.now().getEpochSecond(), maxAgeSec, maxCount, NO_TRUNCATE, acl, metadata, STREAM_ACTIVE);
        StreamMetadata fromDisk = store.get(hash);
        if (fromDisk != null) {
            throw new StreamException("Stream '" + stream + "' already exist");
        }
        return update(streamMeta); //must be called before version
    }

    public void remove(long streamHash) {
        store.remove(streamHash);
    }

    Set<String> matchStreamName(String... pattern) {
        if (pattern == null) {
            return new HashSet<>();
        }
        return match(pattern)
                .map(stream -> stream.name)
                .collect(Collectors.toSet());
    }

    public Set<Long> matchStreamHash(String... pattern) {
        if (pattern == null) {
            return new HashSet<>();
        }
        return match(pattern)
                .map(stream -> stream.hash)
                .collect(Collectors.toSet());
    }

    private Stream<StreamMetadata> match(String... pattern) {
        return Iterators.stream(store.iterator(Direction.FORWARD))
                .map(e -> e.value)
                .filter(stream -> matchAny(stream.name, pattern));
    }

    public static boolean matchAny(String streamName, String... patterns) {
        if (patterns == null) {
            return false;
        }
        for (String pattern : patterns) {
            if (matches(streamName, pattern)) {
                return true;
            }
        }
        return false;
    }

    public static boolean matches(String streamName, String pattern) {
        if (StringUtils.isBlank(pattern) || StringUtils.isBlank(streamName)) {
            return false;
        }
        pattern = pattern.trim();
        boolean startWildcard = pattern.startsWith(WILDCARD);
        boolean endWildcard = pattern.endsWith(WILDCARD);
        if (startWildcard && endWildcard) {
            String patternValue = pattern.substring(1, pattern.length() - 1);
            return !patternValue.isEmpty() && streamName.contains(patternValue);
        }
        if (startWildcard) {
            String patternValue = pattern.substring(1);
            return !patternValue.isEmpty() && streamName.endsWith(patternValue);
        }
        if (endWildcard) {
            String patternValue = pattern.substring(0, pattern.length() - 1);
            return !patternValue.isEmpty() && streamName.startsWith(patternValue);
        }
        return streamName.equals(pattern);
    }

    @Override
    public void close() {
        store.close();
    }

    private void validateName(String streamName) {
        StringUtils.requireNonBlank(streamName, "Stream name must be provided");
        if (streamName.contains(" ")) {
            throw new StreamException("Stream name must not contain whitespaces");
        }
        if (streamName.contains(StreamName.STREAM_VERSION_SEPARATOR)) {
            throw new StreamException("Stream name must not contain " + StreamName.STREAM_VERSION_SEPARATOR);
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

    private StreamMetadata update(StreamMetadata metadata) {
        store.put(metadata.hash, metadata);
        cache.remove(metadata.hash);
        return metadata;
    }

}
