package io.joshworks.eventry.stream;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.utils.StringUtils;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.joshworks.eventry.log.EventRecord.NO_VERSION;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static io.joshworks.eventry.stream.StreamMetadata.STREAM_ACTIVE;

public class Streams implements Closeable {

    public final StreamStore streamStore;

    public Streams(File root, int cacheSize, int cacheMaxAge) {
        this.streamStore = new StreamStore(root, cacheSize, cacheMaxAge);
    }

    public Optional<StreamMetadata> get(String stream) {
        return get(StreamName.hash(stream));
    }

    public Optional<StreamMetadata> get(long streamHash) {
        return Optional.ofNullable(streamStore.get(streamHash));
    }

    public List<StreamMetadata> all() {
        return Iterators.stream(streamStore.iterator(Direction.FORWARD)).map(e -> e.value).collect(Collectors.toList());
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
        StreamMetadata metadata = streamStore.get(streamHash);
        if (metadata == null) {
            metadata = this.createInternal(stream, NO_MAX_AGE, NO_MAX_COUNT, new HashMap<>(), new HashMap<>(), streamHash);
            createdCallback.accept(metadata);
        }
        return metadata;
    }

    public StreamMetadata create(String stream, long maxAge, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata) {
        validateName(stream);
        long hash = StreamName.hash(stream);
        return createInternal(stream, maxAge, maxCount, permissions, metadata, hash);
    }

    //must not hold the lock, since
    private StreamMetadata createInternal(String stream, long maxAge, int maxCount, Map<String, Integer> acl, Map<String, String> metadata, long hash) {
        StreamMetadata streamMeta = new StreamMetadata(stream, hash, System.currentTimeMillis(), maxAge, maxCount, NO_TRUNCATE, acl, metadata, STREAM_ACTIVE);
        streamStore.create(hash, streamMeta); //must be called before version
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
                .filter(stream -> matches(stream.name, prefix));
    }

    public static boolean matches(String streamName, String prefix) {
        return streamName.startsWith(prefix);
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

    public void truncate(StreamMetadata metadata, int currentVersion, int fromVersionInclusive) {
        if (currentVersion <= NO_VERSION) {
            throw new StreamException("Version must be greater or equals zero");
        }
        if (fromVersionInclusive > currentVersion) {
            throw new StreamException("Truncate version: " + fromVersionInclusive + " must be less or equals stream version: " + currentVersion);
        }

        StreamMetadata streamMeta = new StreamMetadata(metadata.name, metadata.hash, metadata.created, metadata.maxAge, metadata.maxCount, fromVersionInclusive, metadata.acl, metadata.metadata, metadata.state);
        streamStore.update(streamMeta);
    }
}
