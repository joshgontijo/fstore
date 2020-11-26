package io.joshworks.es2;

import io.joshworks.fstore.core.io.Channels;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTable {

    private final ByteBuffer data;
    private final Queue<EventEntry> cached = new ArrayDeque<>();
    private final Map<Long, StreamEvents> table = new ConcurrentHashMap<>();
    private final AtomicInteger entries = new AtomicInteger();

    public MemTable(int maxSize, boolean direct) {
        this.data = Buffers.allocate(maxSize, direct);
    }

    public boolean add(ByteBuffer event) {
        assert event.remaining() > 0;
        if (data.remaining() < event.remaining()) {
            return false;
        }

        int offset = data.position();
        int len = event.remaining();
        EventEntry entry = cached.isEmpty() ? new EventEntry() : cached.poll();

        long stream = Event.stream(event);
        int version = Event.version(event);

        entry.offset = offset;
        entry.length = len;
        entry.version = version;

        data.put(event);
        table.computeIfAbsent(stream, k -> new StreamEvents()).add(entry);
        entries.incrementAndGet();
        return true;
    }

    public long get(long stream, int version, SegmentChannel channel) {
        StreamEvents events = table.get(stream);
        if (events == null) {
            return 0;
        }
//        return events.transferTo(channel, version);
        throw new UnsupportedOperationException("TODO");
    }

    public int version(long stream) {
        StreamEvents events = table.get(stream);
        if (events == null) {
            return 0;
        }
        return events.version();
    }

    public long flush(SegmentChannel channel) {
        Set<Long> streams = new TreeSet<>(table.keySet());

        long written = 0;
        for (long stream : streams) {
            written += table.get(stream).flush(channel);
        }

        return written;
    }

    public void clear() {
        for (var key : table.keySet()) {
            StreamEvents entries = table.remove(key);
            entries.clear();
        }
        data.clear();
        entries.set(0);
    }

    public int entries() {
        return entries.get();
    }

    public int size() {
        return data.position();
    }

    private final class StreamEvents {

        private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
        private final List<EventEntry> entries = new ArrayList<>();

        private void add(EventEntry entry) {
            Lock lock = rwLock.writeLock();
            lock.lock();
            try {
                entries.add(entry);
            } finally {
                lock.unlock();
            }
        }

        private void clear() {
            Lock lock = rwLock.writeLock();
            lock.lock();
            try {
                cached.addAll(entries);
                entries.clear();
            } finally {
                lock.unlock();
            }
        }

        private int version() {
            Lock lock = rwLock.readLock();
            lock.lock();
            try {
                return entries.get(entries.size() - 1).version;
            } finally {
                lock.unlock();
            }
        }

        private long flush(SegmentChannel channel) {
            Lock lock = rwLock.readLock();
            lock.lock();
            try {
                long written = 0;
                for (EventEntry entry : entries) {
                    written += channel.append(data.slice(entry.offset, entry.length));
                }
                return written;
            } finally {
                lock.unlock();
            }
        }

        private long transferTo(WritableByteChannel channel, int version) {
            Lock lock = rwLock.readLock();
            lock.lock();
            try {
                int startVersion = entries.get(0).version;
                int endVersion = entries.get(entries.size() - 1).version;

                if (version < startVersion || version > endVersion) {
                    return 0;
                }

                int startIdx = startVersion + (version - startVersion);
                long written = 0;
                for (int i = startIdx; i < entries.size(); i++) {
                    EventEntry entry = entries.get(i);
                    written += Channels.writeFully(channel, data.slice(entry.offset, entry.length));
                }
                return written;
            } finally {
                lock.unlock();
            }
        }
    }

    private static final class EventEntry {
        private int offset;
        private int length;
        private int version;
    }


}


