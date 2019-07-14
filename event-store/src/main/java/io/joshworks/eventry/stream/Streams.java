package io.joshworks.eventry.stream;

import io.joshworks.eventry.LRUCache;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.index.StreamHasher;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.hash.Murmur3Hash;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.joshworks.eventry.log.EventRecord.NO_EXPECTED_VERSION;
import static io.joshworks.eventry.log.EventRecord.NO_VERSION;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static io.joshworks.eventry.stream.StreamMetadata.STREAM_ACTIVE;

public class Streams implements Closeable {

    public static final String STREAM_WILDCARD = "*";
    //TODO LRU cache ? there's no way of getting item by stream name, need to use an indexed lsm-tree
    //LRU map that reads the last version from the index
    private final Map<Long, AtomicInteger> versions;
    public final StreamStore streamStore;
    private final StreamHasher hasher;
    private final Function<Long, Integer> versionFetcher;

    public Streams(File root, int versionLruCacheSize, Function<Long, Integer> versionFetcher) {
        this.versions = new LRUCache<>(versionLruCacheSize);
        this.streamStore = new StreamStore(root, 1000000); //TODO externalize
        this.versionFetcher = versionFetcher;
        //TODO remove ?? StreamNames already has it
        this.hasher = new StreamHasher(new XXHash(), new Murmur3Hash());
    }

    public Optional<StreamMetadata> get(String stream) {
        return get(hashOf(stream));
    }

    public Optional<StreamMetadata> get(long streamHash) {
        return Optional.ofNullable(streamStore.get(streamHash));
    }

    public List<StreamMetadata> all() {
        return Iterators.stream(streamStore.iterator(Direction.FORWARD)).map(e -> e.value).collect(Collectors.toList());
    }

    public long hashOf(String stream) {
        return hasher.hash(stream);
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
        long streamHash = hashOf(stream);
        StreamMetadata metadata = streamStore.get(streamHash);
        if (metadata == null) {
            metadata = this.createInternal(stream, NO_MAX_AGE, NO_MAX_COUNT, new HashMap<>(), new HashMap<>(), streamHash);
            createdCallback.accept(metadata);
        }
        return metadata;
    }

    public StreamMetadata create(String stream, long maxAge, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata) {
        validateName(stream);
        long hash = hashOf(stream);
        return createInternal(stream, maxAge, maxCount, permissions, metadata, hash);
    }

    //must not hold the lock, since
    private StreamMetadata createInternal(String stream, long maxAge, int maxCount, Map<String, Integer> acl, Map<String, String> metadata, long hash) {
        StreamMetadata streamMeta = new StreamMetadata(stream, hash, System.currentTimeMillis(), maxAge, maxCount, NO_TRUNCATE, acl, metadata, STREAM_ACTIVE);
        streamStore.create(hash, streamMeta); //must be called before version
        versions.put(hash, new AtomicInteger(NO_VERSION));
        return streamMeta;
    }

    public boolean remove(long streamHash) {
        return streamStore.remove(streamHash);
    }

    public Set<String> matchStreamName(String prefix) {
        if (prefix == null) {
            return new HashSet<>();
        }
        return match(prefix)
                .map(stream -> stream.name)
                .collect(Collectors.toSet());
    }

    public Set<Long> matchStreamHash(String prefix) {
        if (prefix == null) {
            return new HashSet<>();
        }
        return match(prefix)
                .map(stream -> stream.hash)
                .collect(Collectors.toSet());
    }

    private Stream<StreamMetadata> match(String prefix) {
        return Iterators.stream(streamStore.iterator(Direction.FORWARD))
                .map(e -> e.value)
                .filter(stream -> stream.name.startsWith(prefix));
    }

    public int version(long stream) {
        return getVersion(stream).get();
    }

    private AtomicInteger getVersion(long stream) {
        AtomicInteger counter = versions.get(stream);
        if (counter != null) {
            return counter;
        }
        Integer fromDisk = versionFetcher.apply(stream);//may return NO_VERSION
        AtomicInteger found = new AtomicInteger(fromDisk);
        if (fromDisk > NO_VERSION) {
            versions.put(stream, found);
        }
        return found;
    }

    public int tryIncrementVersion(StreamMetadata metadata, int expected) {
        long hash = metadata.hash;
        AtomicInteger versionCounter = getVersion(hash);
        if (expected <= NO_EXPECTED_VERSION) {
            return versionCounter.incrementAndGet();
        }
        int newValue = expected + 1;
        if (!versionCounter.compareAndSet(expected, newValue)) {
            int currVersion = versionCounter.get();
            throw new StreamException("Version mismatch for '" + metadata.name + "': expected: " + expected + " current :" + currVersion);
        }
        return newValue;
    }

    @Override
    public void close() {
        streamStore.close();
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

    public void truncate(StreamMetadata metadata, int fromVersionInclusive) {
        int currentVersion = version(metadata.hash);
        if (currentVersion <= NO_VERSION) {
            throw new StreamException("Version must be greater or equals zero");
        }
        int streamVersion = version(metadata.hash);
        if (fromVersionInclusive > streamVersion) {
            throw new StreamException("Truncate version: " + fromVersionInclusive + " must be less or equals stream version: " + streamVersion);
        }

        StreamMetadata streamMeta = new StreamMetadata(metadata.name, metadata.hash, metadata.created, metadata.maxAge, metadata.maxCount, fromVersionInclusive, metadata.acl, metadata.metadata, metadata.state);
        streamStore.update(streamMeta);
    }
}
