package io.joshworks.eventry.log.cache;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.fstore.core.Serializer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

//TODO LRU cache as well ???
public class EventRecordCache implements Closeable {

    public static final int NO_CACHING = 0;
    public static final int CACHING_NO_MAX_AGE = -1;

    private final Serializer<EventRecord> serializer = new EventSerializer();
    private final long maxSize;
    private final long maxAge;
    private final AtomicLong available = new AtomicLong();
    private final Map<Long, CachedEntry> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor;

    public EventRecordCache(long maxSize, long maxAge) {
        this.maxSize = maxSize;
        this.maxAge = maxAge;
        if (maxAge == CACHING_NO_MAX_AGE) {
            this.executor = null;
            return;
        }
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.executor.scheduleAtFixedRate(this::evict, 1, 1, TimeUnit.MINUTES);
    }

    public static EventRecordCache instance(long maxSize, long maxAge) {
        return maxSize > NO_CACHING ? new EventRecordCache(maxSize, maxAge) : new NoOpCache();
    }

    private void evict() {
        long now = System.currentTimeMillis();
        cache.entrySet().removeIf(kv -> now - kv.getValue().created() > maxAge);
    }

    public EventRecord get(long position) {
        CachedEntry entry = cache.get(position);
        if (entry == null) {
            return null;
        }
        return serializer.fromBytes(entry.get());
    }

    public boolean cache(long position, EventRecord entry) {
        ByteBuffer data = serializer.toBytes(entry);
        long newSize = available.updateAndGet(val -> val < maxSize ? val + data.limit() : val);
        if (newSize > maxSize) {
            return false;
        }
        ByteBuffer cacheData = ByteBuffer.allocateDirect(data.limit());
        cacheData.put(data);
        cache.put(position, new CachedEntry(data));
        return true;
    }

    void remove(long position) {
        CachedEntry removed = cache.remove(position);
        if (removed == null) {
            return;
        }

        ByteBuffer buffer = removed.get();
        available.updateAndGet(val -> val < maxSize ? val - buffer.limit() : val);
    }

    @Override
    public void close() {
        cache.clear();
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static class NoOpCache extends EventRecordCache {

        private NoOpCache() {
            super(NO_CACHING, CACHING_NO_MAX_AGE);
        }

        @Override
        public EventRecord get(long position) {
            return null;
        }

        @Override
        public boolean cache(long position, EventRecord entry) {
            return false;
        }

        @Override
        void remove(long position) {
            //do nothing
        }

        @Override
        public void close() {
            //do nothing
        }
    }

}
