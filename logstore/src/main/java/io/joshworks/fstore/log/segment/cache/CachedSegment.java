package io.joshworks.fstore.log.segment.cache;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Marker;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class CachedSegment<T> implements Log<T> {

    private final CacheManager cacheManager;
    private final Log<T> delegate;
    private final Serializer<T> serializer;

    private final Cache cache;

    public CachedSegment(Log<T> delegate, Serializer<T> serializer, CacheManager cacheManager) {
        this.delegate = delegate;
        this.serializer = serializer;
        this.cacheManager = cacheManager;
        this.cache = cacheManager.createCache();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Stream<T> stream(Direction direction) {
        return Iterators.closeableStream(iterator(direction));
    }

    @Override
    public LogIterator<T> iterator(long position, Direction direction) {
        return new CachedIterator(delegate.iterator(position, direction));
    }

    @Override
    public LogIterator<T> iterator(Direction direction) {
        return new CachedIterator(delegate.iterator(direction));
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public Marker marker() {
        return delegate.marker();
    }

    private T getOrCache(long position, Function<Long, T> provider) {
        CachedEntry cached = cache.get(position);
        if (cached != null) {
            ByteBuffer buffer = cached.buffer;
            buffer.clear();
            return serializer.fromBytes(buffer);
        }
        T entry = provider.apply(position);
        if (entry == null) {
            return null;
        }

        ByteBuffer entryBuffer = serializer.toBytes(entry);
        if (cacheManager.allocateCache(entryBuffer.remaining())) {
            ByteBuffer newCachedBuffer = ByteBuffer.allocateDirect(entryBuffer.limit());
            newCachedBuffer.put(entryBuffer);
            cache.put(position, new CachedEntry(newCachedBuffer));
        }

        return entry;

    }

    @Override
    public T get(long position) {
        return getOrCache(position, delegate::get);
    }

    @Override
    public PollingSubscriber<T> poller(long position) {
        return new CachedPoller(delegate.poller(position));
    }

    @Override
    public PollingSubscriber<T> poller() {
        return new CachedPoller(delegate.poller());
    }

    @Override
    public long fileSize() {
        return delegate.fileSize();
    }

    @Override
    public long actualSize() {
        return delegate.actualSize();
    }

    @Override
    public Set<TimeoutReader> readers() {
        return delegate.readers();
    }

    @Override
    public SegmentState rebuildState(long lastKnownPosition) {
        return delegate.rebuildState(lastKnownPosition);
    }

    @Override
    public void delete() {
        delegate.delete();
    }

    @Override
    public void roll(int level) {
        delegate.roll(level);
    }

    @Override
    public void roll(int level, ByteBuffer footer) {
        delegate.roll(level, footer);
    }

    @Override
    public ByteBuffer readFooter() {
        return delegate.readFooter();
    }

    @Override
    public boolean readOnly() {
        return delegate.readOnly();
    }

    @Override
    public long entries() {
        return delegate.entries();
    }

    @Override
    public int level() {
        return delegate.level();
    }

    @Override
    public long created() {
        return delegate.created();
    }

    @Override
    public long append(T data) {
        return delegate.append(data);
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        cacheManager.removeCache(cache);
    }

    public static <T> SegmentFactory<T> factory(SegmentFactory<T> delegate, CacheManager cacheManager) {
        return new CachedSegmentFactory<>(delegate, cacheManager);
    }

    private class CachedIterator implements LogIterator<T> {

        private final LogIterator<T> it;

        private CachedIterator(LogIterator<T> delegate) {
            this.it = delegate;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            long pos = position();
            return getOrCache(pos, p -> it.next());
        }

        @Override
        public long position() {
            return it.position();
        }

        @Override
        public void close() throws IOException {
            it.close();
        }
    }

    private class CachedPoller implements PollingSubscriber<T> {

        private final PollingSubscriber<T> poller;

        private CachedPoller(PollingSubscriber<T> poller) {
            this.poller = poller;
        }

        private Function<Long, T> wrapping(Supplier2<T> original) {
            return p -> original.get();
        }

        @Override
        public T peek() {
            long pos = position();
            return getOrCache(pos, wrapping(poller::peek));
        }

        @Override
        public T poll() {
            long pos = position();
            return getOrCache(pos, wrapping(poller::poll));
        }

        @Override
        public T poll(long limit, TimeUnit timeUnit) {
            long pos = position();
            return getOrCache(pos, wrapping(() -> poller.poll(limit, timeUnit)));
        }

        @Override
        public T take() {
            long pos = position();
            return getOrCache(pos, wrapping(poller::take));
        }

        @Override
        public boolean headOfLog() {
            return poller.headOfLog();
        }

        @Override
        public boolean endOfLog() {
            return poller.endOfLog();
        }

        @Override
        public long position() {
            return poller.position();
        }

        @Override
        public void close() throws IOException {
            poller.close();
        }
    }

    private static class CachedSegmentFactory<T> implements SegmentFactory<T> {

        private final SegmentFactory<T> delegate;
        private final CacheManager cacheManager;

        private CachedSegmentFactory(SegmentFactory<T> delegate, CacheManager cacheManager) {
            this.delegate = delegate;
            this.cacheManager = cacheManager;
        }

        @Override
        public Log<T> createOrOpen(Storage storage, Serializer<T> serializer, IDataStream reader, String magic, Type type) {
            Log<T> segment = delegate.createOrOpen(storage, serializer, reader, magic, type);
            return new CachedSegment<>(segment, serializer, cacheManager);
        }
    }

    @FunctionalInterface
    public interface Supplier2<T> extends Supplier<T> {

        @Override
        default T get() {
            try {
                return getThrows();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        T getThrows() throws Exception;

    }

}
