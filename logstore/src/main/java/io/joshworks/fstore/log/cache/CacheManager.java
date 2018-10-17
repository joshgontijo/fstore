package io.joshworks.fstore.log.cache;


import java.io.Closeable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

public class CacheManager implements Closeable {

    public static final int NO_CACHING = -1;
    public static final int CACHING_NO_MAX_AGE = -1;

    private final long maxSize;
    private final int maxAge;
    private final AtomicLong cacheAvailable = new AtomicLong();
    private final ScheduledExecutorService executor;

    private final Set<Cache> caches = new HashSet<>();


    public CacheManager(long maxSize, int maxAge) {
        if (maxSize == NO_CACHING) {
            this.maxSize = NO_CACHING;
            this.maxAge = CACHING_NO_MAX_AGE;
            this.executor = null;
            return;
        }
        this.maxAge = maxAge;
        this.maxSize = maxSize;
        if (maxAge != CACHING_NO_MAX_AGE) {
            this.executor = Executors.newSingleThreadScheduledExecutor();
            this.executor.scheduleAtFixedRate(this::evict, 1, 1, TimeUnit.MINUTES);
            return;
        }
        this.executor = null;
    }

    boolean allocateCache(long size) {
        long newSize = cacheAvailable.updateAndGet(val -> val < maxSize ? val + size : val);
        return newSize < maxSize;
    }

    void free(long size) {
        cacheAvailable.updateAndGet(val -> val < maxSize ? val - size : val);
    }

    Cache createCache() {
        Cache cache = new TrackedCache();
        caches.add(cache);
        return cache;
    }

    void removeCache(Cache cache) {
        if (caches.remove(cache)) {
            free(cache.size());
        }
    }

    private void evict() {
        for (Cache item : caches) {
            long nowSec = System.currentTimeMillis() / 1000;
            item.removeIf(entry -> nowSec - entry.timestamp > maxAge);
        }
    }


    @Override
    public void close() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    public static class TrackedCache implements Cache {

        private final Map<Long, CachedEntry> map = new ConcurrentHashMap<>();

        private TrackedCache() {

        }

        @Override
        public CachedEntry get(long position) {
            return map.get(position);
        }

        @Override
        public void put(long position, CachedEntry cachedEntry) {
            map.put(position, cachedEntry);
        }

        @Override
        public void removeIf(Predicate<CachedEntry> predicate) {
            map.entrySet().removeIf(kv -> predicate.test(kv.getValue()));
        }

        @Override
        public long size() {
            return map.values().stream().mapToLong(c -> c.buffer.capacity()).sum();
        }

        @Override
        public int entries() {
            return map.size();
        }
    }


}
