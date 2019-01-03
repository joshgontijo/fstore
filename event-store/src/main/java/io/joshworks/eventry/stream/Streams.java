package io.joshworks.eventry.stream;

import io.joshworks.eventry.LRUCache;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.index.StreamHasher;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.hash.Murmur3Hash;
import io.joshworks.fstore.core.hash.XXHash;

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

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static io.joshworks.eventry.stream.StreamMetadata.STREAM_ACTIVE;

public class Streams implements Closeable {

    public static final String STREAM_WILDCARD = "*";
    //TODO LRU cache ? there's no way of getting item by stream name, need to use an indexed lsm-tree
    //LRU map that reads the last version from the index
    private final Map<Long, AtomicInteger> versions;
    private final StreamStore streamStore;
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
        return streamStore.stream().map(e -> e.value).collect(Collectors.toList());
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
    public synchronized StreamMetadata createIfAbsent(String stream, Consumer<StreamMetadata> createdCallback) {
        validateName(stream);
        long streamHash = hashOf(stream);
        StreamMetadata metadata = streamStore.get(streamHash);
        if (metadata == null) {
            metadata = this.createInternal(stream, NO_MAX_AGE, NO_MAX_COUNT, new HashMap<>(), new HashMap<>(), streamHash);
            createdCallback.accept(metadata);
        }
        return metadata;
    }

    public synchronized StreamMetadata create(String stream, long maxAge, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata) {
        validateName(stream);
        long hash = hashOf(stream);
        return createInternal(stream, maxAge, maxCount, permissions, metadata, hash);
    }

    //must not hold the lock, since
    private StreamMetadata createInternal(String stream, long maxAge, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata, long hash) {
        StreamMetadata streamMeta = new StreamMetadata(stream, hash, System.currentTimeMillis(), maxAge, maxCount, NO_TRUNCATE, permissions, metadata, STREAM_ACTIVE);
        streamStore.create(hash, streamMeta); //must be called before version
        versions.put(hash, new AtomicInteger(NO_VERSION));
        return streamMeta;
    }

    public StreamMetadata remove(long streamHash) {
        return streamStore.remove(streamHash);
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
            return streamStore.stream()
                    .map(e -> e.value)
                    .filter(stream -> stream.name.startsWith(prefix))
                    .map(stream -> stream.name)
                    .collect(Collectors.toSet());
        }

        return streamStore.stream()
                .map(e -> e.value)
                .filter(stream -> stream.name.equals(value))
                .map(stream -> stream.name)
                .collect(Collectors.toSet());


    }

    public int version(long stream) {
        return getVersion(stream).get();
    }

    private AtomicInteger getVersion(long stream) {
        AtomicInteger counter = versions.get(stream);
        if (counter != null) {
            return counter;
        }
        Integer fromDisk = versionFetcher.apply(stream);//may return -1 (NO_VERSION)
        AtomicInteger found = new AtomicInteger(fromDisk);
        versions.put(stream, found);
        return found;
    }

    //lock not required, it uses getVersion
    public int tryIncrementVersion(long stream, int expected) {
        AtomicInteger versionCounter = getVersion(stream);
        if (expected <= NO_VERSION) {
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
        streamStore.close();
    }

    private void validateName(String streamName) {
        StringUtils.requireNonBlank(streamName, "Stream name must be provided");
        if(streamName.contains(" ")) {
            throw new IllegalArgumentException("Stream name must not contain whitespaces");
        }
        if(streamName.contains(StreamName.STREAM_VERSION_SEPARATOR)) {
            throw new IllegalArgumentException("Stream name must not contain " + StreamName.STREAM_VERSION_SEPARATOR);
        }
    }

    public void truncate(String stream, int version) {
        StreamMetadata metadata = get(stream).orElseThrow(() -> new IllegalArgumentException("No metadata found for stream " + stream));
        int currentVersion = version(metadata.hash);
        if (currentVersion <= NO_VERSION) {
            throw new IllegalArgumentException("Version must be greater or equals zero");
        }
        int streamVersion = version(metadata.hash);
        if (version > streamVersion) {
            throw new IllegalArgumentException("Truncate version: " + version + " must be less or equals stream version: " + streamVersion);
        }

        StreamMetadata streamMeta = new StreamMetadata(metadata.name, metadata.hash, metadata.created, metadata.maxAge, metadata.maxCount, version, metadata.permissions, metadata.metadata, metadata.state);
        streamStore.update(streamMeta);
    }
}
