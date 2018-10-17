//package io.joshworks.fstore.log.appender;
//
//import io.joshworks.fstore.core.io.Storage;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//
////Not thread safe
//public class CachedReadStorage implements Storage {
//
//    private final Storage delegate;
//    private final long maxAge;
//    private final StorageProvider storageProvider;
//    private final Map<Long, CachedEntry> cache = new ConcurrentHashMap<>();
//
//    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
//
//    public CachedReadStorage(Storage delegate, long maxAge, StorageProvider storageProvider) {
//        this.delegate = delegate;
//        this.maxAge = maxAge;
//        this.storageProvider = storageProvider;
//        scheduler.scheduleAtFixedRate(this::evict, 1, 1, TimeUnit.MINUTES);
//    }
//
//    private void evict() {
//        long nowSec = System.currentTimeMillis() / 1000;
//        cache.entrySet().removeIf(kv -> nowSec - kv.getValue().timestamp > maxAge);
//    }
//
//    @Override
//    public int write(ByteBuffer data) {
////        int dataSize = data.remaining();
////        long position = delegate.position();
////        if (storageProvider.allocateCache(dataSize)) {
////            ByteBuffer cached = ByteBuffer.allocateDirect(dataSize);
////            cached.put(data);
////            cache.put(position, new CachedEntry(cached));
////        }
//
//        return delegate.write(data);
//    }
//
//    @Override
//    public int read(long position, ByteBuffer data) {
//        CachedEntry cachedEntry = cache.get(position);
//        if (cachedEntry != null) {
//            ByteBuffer cached = cachedEntry.buffer.clear();
//            int size = Math.min(cached.remaining(), data.remaining());
//            cached.limit(size);
//            data.put(cached);
//
//            if(data.hasRemaining()) { //cached has less than the required cache
//                long newPos = position + data.remaining();
//                delegate.read(newPos, data);
//            }
//
//            return size;
//        }
//        int posBefore = data.position();
//        int read = delegate.read(position, data);
//        int posAfter = data.position();
//
//        if(read > 0) {
//            ByteBuffer pos = data.position(posBefore);
//            ByteBuffer.allocateDirect(posAfter - )
//        }
//
//
//
//    }
//
//    @Override
//    public long length() {
//        return delegate.length();
//    }
//
//    @Override
//    public void position(long position) {
//        delegate.position(position);
//    }
//
//    @Override
//    public long position() {
//        return delegate.position();
//    }
//
//    @Override
//    public void delete() {
//        delegate.delete();
//    }
//
//    @Override
//    public String name() {
//        return delegate.name();
//    }
//
//    @Override
//    public void truncate(long pos) {
//        cache.entrySet().removeIf(kv -> {
//            long entryPos = kv.getKey();
//            long entryEnd = pos + kv.getValue().buffer.clear().limit();
//            return entryPos > pos || entryEnd > pos;
//        });
//        delegate.truncate(pos);
//    }
//
//    @Override
//    public void markAsReadOnly() {
//        delegate.markAsReadOnly();
//    }
//
//    @Override
//    public void flush() throws IOException {
//        delegate.flush();
//    }
//
//    @Override
//    public void close() throws IOException {
//        for (CachedEntry cached : cache.values()) {
//            storageProvider.freeCache(cached.buffer.capacity());
//        }
//        cache.clear();
//        delegate.close();
//    }
//
//
//}