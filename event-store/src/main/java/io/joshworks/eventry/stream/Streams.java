package io.joshworks.eventry.stream;

import io.joshworks.eventry.LRUCache;
import io.joshworks.eventry.data.Constant;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.StreamHasher;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.hash.Murmur3Hash;
import io.joshworks.fstore.core.hash.XXHash;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.STREAM_ACTIVE;

public class Streams implements Closeable {

    public static final String STREAM_WILDCARD = "*";
    public static final String ALL_STREAM = Constant.SYSTEM_PREFIX + "all";
    //TODO LRU cache ? there's no way of getting item by stream name, need to use an indexed lsm-tree
    //LRU map that reads the last version from the index
    private final LRUCache<Long, AtomicInteger> versions;
    private final Map<Long, StreamMetadata> streamsMap = new ConcurrentHashMap<>();
    private final StreamHasher hasher;

    public Streams(int versionLruCacheSize, Function<Long, Integer> versionFetcher) {
        this.versions = new LRUCache<>(versionLruCacheSize, streamHash -> new AtomicInteger(versionFetcher.apply(streamHash)));
        this.hasher = new StreamHasher(new XXHash(), new Murmur3Hash());
    }

    public Optional<StreamMetadata> get(String stream) {
        return get(hashOf(stream));
    }

    public Optional<StreamMetadata> get(long streamHash) {
        return Optional.ofNullable(streamsMap.get(streamHash));
    }

    public List<StreamMetadata> all() {
        return new ArrayList<>(streamsMap.values());
    }

    public long hashOf(String stream) {
        return hasher.hash(stream);
    }

    public void add(StreamMetadata metadata) {
        Objects.requireNonNull(metadata, "Metadata must be provided");
        StringUtils.requireNonBlank(metadata.name, "Stream name is empty");
        StreamMetadata existing = streamsMap.putIfAbsent(metadata.hash, metadata);
        if (existing != null) {
            throw new IllegalStateException("Stream '" + metadata.name + "' already exist: " + existing);
        }
    }

    public StreamMetadata create(String stream) {
        return create(stream, NO_MAX_AGE, NO_MAX_COUNT);
    }

    public StreamMetadata create(String stream, long maxAge, int maxCount) {
        return create(stream, maxAge, maxCount, new HashMap<>(), new HashMap<>());
    }

    public StreamMetadata create(String stream, long maxAge, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata) {
        StringUtils.requireNonBlank(stream, "Stream must be provided");
        long hash = hashOf(stream);
        StreamMetadata streamMeta = new StreamMetadata(stream, hash, System.currentTimeMillis(), maxAge, maxCount, permissions, metadata, STREAM_ACTIVE);
        versions.set(hash, new AtomicInteger(IndexEntry.NO_VERSION));
        StreamMetadata existing = streamsMap.putIfAbsent(hash, streamMeta);
        if (existing != null) {
            return null;
        }
        return streamMeta;
    }

    public StreamMetadata remove(long streamHash) {
        return streamsMap.remove(streamHash);
    }

    //Only supports 'startingWith' wildcard
    //EX: users-*
    public Set<String> streamMatching(String value) {
        if (value == null) {
            return new HashSet<>();
        }
        //wildcard
        if (value.endsWith(STREAM_WILDCARD)) {
            final String prefix = value.substring(0, value.length() - 1);
            return streamsMap.values().stream()
                    .filter(stream -> stream.name.startsWith(prefix))
                    .map(stream -> stream.name)
                    .collect(Collectors.toSet());
        }

        return streamsMap.values().stream()
                .filter(stream -> stream.name.equals(value))
                .map(stream -> stream.name)
                .collect(Collectors.toSet());


    }

    public int version(long stream) {
        return versions.getOrElse(stream, new AtomicInteger(IndexEntry.NO_VERSION)).get();
    }

    public int tryIncrementVersion(long stream, int expected) {
        AtomicInteger versionCounter = versions.getOrElse(stream, new AtomicInteger(IndexEntry.NO_VERSION));
        if (expected < 0) {
            return versionCounter.incrementAndGet();
        }
        int newValue = expected + 1;
        if (!versionCounter.compareAndSet(expected, newValue)) {
            throw new IllegalArgumentException("Version mismatch: expected stream " + stream + " version is higher than expected: " + expected);
        }
        return newValue;
    }


    @Override
    public void close() {

    }

}
