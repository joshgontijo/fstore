package io.joshworks.es2;

import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.BlockCodec;
import io.joshworks.es2.sstable.StreamBlock;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Memory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTable {

    private static final int MAX_READ_SIZE = Memory.PAGE_SIZE;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ByteBuffer data;
    private final Queue<EventEntry> cached = new ArrayDeque<>();
    private final Map<Long, StreamEvents> table = new ConcurrentHashMap<>();
    private final AtomicInteger entries = new AtomicInteger();

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> Buffers.allocate(MAX_READ_SIZE, false));

    public MemTable(int maxSize, boolean direct) {
        this.data = Buffers.allocate(maxSize, direct);
    }

    public boolean add(ByteBuffer event) {
        assert event.remaining() > 0;
        if (data.remaining() < event.remaining()) {
            return false;
        }

        EventEntry entry = cached.isEmpty() ? new EventEntry() : cached.poll();

        long stream = Event.stream(event);
        int version = Event.version(event);

        entry.offset = data.position();
        entry.length = event.remaining();
        entry.version = version;

        data.put(event);
        table.computeIfAbsent(stream, k -> new StreamEvents(stream, version)).add(entry);
        entries.incrementAndGet();
        return true;
    }

    public int get(long stream, int version, Sink sink) {
        StreamEvents events = table.get(stream);
        if (events == null) {
            return Event.NO_VERSION;
        }

        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            return events.writeTo(version, sink);
        } finally {
            lock.unlock();
        }
    }

    public int version(long stream) {
        StreamEvents events = table.get(stream);
        if (events == null) {
            return Event.NO_VERSION;
        }
        return events.version();
    }

//    public void flush(SSTables sstables) {
//        var watch = TimeWatch.start();
//        var it = new MemTableFlushIterator();
//        sstables.flush(it, entries.get());
//        System.out.println("Flushed " + entries() + " entries (" + size() + " bytes) in " + watch.elapsed() + "ms");
//        clear();
//    }

    public void clear() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            for (var key : table.keySet()) {
                StreamEvents entries = table.remove(key);
                entries.clear();
            }
            data.clear();
            entries.set(0);
        } finally {
            lock.unlock();
        }
    }

    public int entries() {
        return entries.get();
    }

    public int size() {
        return data.position();
    }

    public Iterator<ByteBuffer> flushIterator() {
        return new MemTableFlushIterator();
    }

    private final class StreamEvents {

        private final ConcurrentLinkedQueue<EventEntry> entries = new ConcurrentLinkedQueue<>();
        private final AtomicInteger version = new AtomicInteger(Event.NO_VERSION);
        private final long stream;
        private final int startVersion;

        private StreamEvents(long stream, int startVersion) {
            this.stream = stream;
            this.startVersion = startVersion;
        }

        private void add(EventEntry entry) {
            entries.add(entry);
            int prevVersion = version.getAndSet(entry.version);
            assert prevVersion == Event.NO_VERSION || prevVersion + 1 == entry.version;
        }

        private void clear() {
            cached.addAll(entries);
            entries.clear();
        }

        private int version() {
            return version.get();
        }

        private int writeTo(int fromVersion, Sink sink) {
            if (fromVersion >= startVersion + entries.size()) {
                return Event.VERSION_TOO_HIGH;
            }
            if (fromVersion < startVersion) {
                return Event.NO_VERSION;
            }

            ByteBuffer buff = writeBuffer.get();
            buff.clear();
            buff.position(StreamBlock.HEADER_BYTES);

            int size = 0;
            int entryCount = 0;
            for (EventEntry entry : entries) {
                if (entry.version < fromVersion) {
                    continue;
                }
                if (buff.remaining() < entry.length) {
                    break;
                }
                size += Buffers.copy(data, entry.offset, entry.length, buff);
                entryCount++;
            }
            assert size > 0;

            buff.flip();//write header does not modify buffer's position so it's safe
            StreamBlock.writeHeader(buff, stream, fromVersion, entryCount, size, BlockCodec.NONE);
            return sink.write(buff);
        }
    }

    private static final class EventEntry {
        private int offset;
        private int length;
        private int version;
    }

    private class MemTableFlushIterator implements Iterator<ByteBuffer> {

        private final Iterator<Long> streams;
        private Iterator<EventEntry> currStreamIt;

        public MemTableFlushIterator() {
            this.streams = new TreeSet<>(table.keySet()).iterator();
        }

        @Override
        public boolean hasNext() {
            return (currStreamIt != null && currStreamIt.hasNext()) || streams.hasNext();
        }

        @Override
        public ByteBuffer next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (currStreamIt == null || !currStreamIt.hasNext()) {
                long stream = streams.next();
                currStreamIt = table.get(stream).entries.iterator();
            }
            EventEntry entry = currStreamIt.next();
            return data.slice(entry.offset, entry.length);
        }
    }

}


