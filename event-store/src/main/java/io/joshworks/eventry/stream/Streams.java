package io.joshworks.eventry.stream;

import io.joshworks.eventry.LRUCache;
import io.joshworks.eventry.data.Constant;
import io.joshworks.eventry.index.IndexEntry;
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

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.STREAM_ACTIVE;

public class Streams implements Closeable {

    public static final String STREAM_WILDCARD = "*";
    public static final String ALL_STREAM = Constant.SYSTEM_PREFIX + "all";
    //TODO LRU cache ? there's no way of getting item by stream name, need to use an indexed lsm-tree
    //LRU map that reads the last version from the index
    private final Map<Long, AtomicInteger> versions;
    private final StreamStore streamStore;
    private final StreamHasher hasher;
    private final Function<Long, Integer> versionFetcher;

    private final EquivalenceLock<Long> streamLock = new EquivalenceLock<>();

    public Streams(File root, int versionLruCacheSize, Function<Long, Integer> versionFetcher) {
        this.versions = new LRUCache<>(versionLruCacheSize);
        this.streamStore = new StreamStore(root, 1000000); //TODO externalize
        this.versionFetcher = versionFetcher;
        this.hasher = new StreamHasher(new XXHash(), new Murmur3Hash());
    }

    @Deprecated
    public Optional<StreamMetadata> get(String stream) {
        return get(hashOf(stream));
    }

    @Deprecated
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
    public StreamMetadata createIfAbsent(String stream, Consumer<StreamMetadata> createdCallback) {
        long streamHash = hashOf(stream);
        streamLock.lock(streamHash);
        try {
            StreamMetadata metadata = streamStore.get(streamHash);
            if (metadata == null) {
                metadata = this.createInternal(stream, NO_MAX_AGE, NO_MAX_COUNT, new HashMap<>(), new HashMap<>(), streamHash);
                createdCallback.accept(metadata);
            }
            return metadata;
        } finally {
            streamLock.unlock(streamHash);
        }
    }

    public StreamMetadata create(String stream, long maxAge, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata) {
        StringUtils.requireNonBlank(stream, "Stream must be provided");
        long hash = hashOf(stream);

        streamLock.lock(hash);
        try {
            return createInternal(stream, maxAge, maxCount, permissions, metadata, hash);
        } finally {
            streamLock.unlock(hash);
        }
    }

    //must not hold the lock, since
    private StreamMetadata createInternal(String stream, long maxAge, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata, long hash) {
        StreamMetadata streamMeta = new StreamMetadata(stream, hash, System.currentTimeMillis(), maxAge, maxCount, permissions, metadata, STREAM_ACTIVE);
        streamStore.put(hash, streamMeta); //must be called before version
        versions.put(hash, new AtomicInteger(IndexEntry.NO_VERSION));
        return streamMeta;
    }

    public StreamMetadata remove(long streamHash) {
        streamLock.lock(streamHash);
        try {
            return streamStore.remove(streamHash);
        } finally {
            streamLock.unlock(streamHash);
        }
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
        streamLock.lock(stream);
        try {
            AtomicInteger counter = versions.get(stream);
            if (counter != null) {
                return counter;
            }
            Integer fromDisk = versionFetcher.apply(stream);//may return -1 (NO_VERSION)
            AtomicInteger found = new AtomicInteger(fromDisk);
            versions.put(stream, found);
            return found;
        } finally {
            streamLock.unlock(stream);
        }
    }

    //lock not required, it uses getVersion
    public int tryIncrementVersion(long stream, int expected) {
        AtomicInteger versionCounter = getVersion(stream);
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
        streamStore.close();
    }


    private static class EquivalenceLock<T> {
        private final Set<T> slots = new HashSet<>();

        public void lock(final T ticket) {
            synchronized (slots) {
//                String name = Thread.currentThread().getName();
//                System.out.println("[" + name + "] Acquiring lock for " + ticket);
                while (slots.contains(ticket)) {
                    try {
//                        System.out.println("[" + name + "] awaiting lock for " + ticket);
                        slots.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Failed to acquire lock for " + ticket, e);
                    }
                }
                slots.add(ticket);
            }
        }

        public boolean tryLock(final T ticket) {
            synchronized (slots) {
                if (slots.contains(ticket)) {
                    return false;
                }
                slots.add(ticket);
                return true;
            }
        }

        public void unlock(final T ticket) {
            synchronized (slots) {
//                String name = Thread.currentThread().getName();
//                System.out.println("[" + name + "] Lock released for " + ticket);
                slots.remove(ticket);
                slots.notifyAll();
            }
        }
    }

}
