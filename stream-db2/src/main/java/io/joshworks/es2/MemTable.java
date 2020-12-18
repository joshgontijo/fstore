package io.joshworks.es2;

import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.BlockCodec;
import io.joshworks.es2.sstable.SSTables;
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

public class MemTable {

    private static final int MAX_READ_SIZE = Memory.PAGE_SIZE;

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

        int offset = data.position();
        int len = event.remaining();
        EventEntry entry = cached.isEmpty() ? new EventEntry() : cached.poll();

        long stream = Event.stream(event);
        int version = Event.version(event);

        entry.offset = offset;
        entry.length = len;
        entry.version = version;

        data.put(event);
        table.computeIfAbsent(stream, k -> new StreamEvents(stream, version)).add(entry);
        entries.incrementAndGet();
        return true;
    }

    public int get(long stream, int version, Sink sink) {
        StreamEvents events = table.get(stream);
        if (events == null) {
            return 0;
        }

        return events.writeTo(version, sink);
    }

    public int version(long stream) {
        StreamEvents events = table.get(stream);
        if (events == null) {
            return 0;
        }
        return events.version();
    }

    public long flush(SSTables sstables) {

        MemTableFLushIterator it = new MemTableFLushIterator();
        sstables.flush(it);
        return size();
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

        private final ConcurrentLinkedQueue<EventEntry> entries = new ConcurrentLinkedQueue<>();
        private final AtomicInteger version = new AtomicInteger(-1);
        private final long stream;
        private final int startVersion;

        private StreamEvents(long stream, int startVersion) {
            this.stream = stream;
            this.startVersion = startVersion;
        }

        private void add(EventEntry entry) {
            assert entry.version == version.get() + 1 : "Non contiguous version";
            entries.add(entry);
            version.incrementAndGet();
        }

        private void clear() {
            cached.addAll(entries);
            entries.clear();
        }

        private int version() {
            return version.get();
        }

        private long flush(SegmentChannel channel) {
            long written = 0;
            for (EventEntry entry : entries) {
                written += channel.append(data.slice(entry.offset, entry.length));
            }
            return written;
        }

        private int writeTo(int fromVersion, Sink sink) {
            if (fromVersion < startVersion || fromVersion >= startVersion + entries.size()) {
                return 0;
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

    private class MemTableFLushIterator implements Iterator<ByteBuffer> {

        private final Iterator<Long> streams;
        private Iterator<EventEntry> currStreamIt;

        public MemTableFLushIterator() {
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

