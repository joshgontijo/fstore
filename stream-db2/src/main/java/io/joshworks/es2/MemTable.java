package io.joshworks.es2;

import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.BlockCodec;
import io.joshworks.es2.sstable.StreamBlock;
import io.joshworks.fstore.core.io.Channels;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MemTable {

    private final ByteBuffer data;
    private final Map<Long, StreamEvents> table = new ConcurrentHashMap<>();
    private final AtomicInteger entries = new AtomicInteger();

    private final AtomicInteger readers = new AtomicInteger();

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> Buffers.allocate(Memory.PAGE_SIZE, false));

    public MemTable(int maxSize, boolean direct) {
        this.data = Buffers.allocate(maxSize, direct);
    }

    public boolean add(ByteBuffer event) {
        assert event.remaining() > 0;
        assert Event.version(event) >= 0;

        if (data.remaining() < event.remaining()) {
            return false;
        }

        EventEntry entry = new EventEntry();

        long stream = Event.stream(event);
        int version = Event.version(event);

        entry.offset = data.position();
        entry.size = event.remaining();
        entry.version = version;

        var copied = Buffers.copy(event, data);
        assert entry.size == copied;
        table.computeIfAbsent(stream, k -> new StreamEvents(stream, version)).add(entry);
        entries.incrementAndGet();
        return true;
    }

    public int get(long stream, int version, Sink sink) {
        try {
            readers.incrementAndGet();
            StreamEvents events = table.get(stream);
            if (events == null) {
                return Event.NO_VERSION;
            }
            return events.writeTo(version, sink);
        } finally {
            readers.decrementAndGet();
        }
    }

    public int version(long stream) {
        StreamEvents events = table.get(stream);
        return events == null ? Event.NO_VERSION : events.version();
    }

    public void clear() {
        while (readers.get() > 0) {
            Thread.yield(); //wait for readers to complete
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

    public Iterator<ByteBuffer> flushIterator() {
        return new MemTableFlushIterator();
    }

    private final class StreamEvents {

        private final List<EventEntry> entries = new ArrayList<>();
        private final long stream;
        private final int startVersion;

        private StreamEvents(long stream, int startVersion) {
            this.stream = stream;
            this.startVersion = startVersion;
        }

        private void add(EventEntry entry) {
            assert entry.version == version() + 1;
            entries.add(entry);
        }

        private int version() {
            return startVersion + entries.size() - 1;
        }

        private int writeTo(int fromVersion, Sink sink) {
            if (fromVersion > version()) {
                return Event.VERSION_TOO_HIGH;
            }
            if (fromVersion < startVersion) {
                return Event.NO_VERSION;
            }

            var startIdx = fromVersion - startVersion;

            int totalBytes = 0;
            int totalEntries = entries.size() - startIdx;
            for (int idx = startIdx; idx < totalEntries; idx++) {
                totalBytes += entries.get(idx).size;
            }


            ByteBuffer buff = writeBuffer.get();
            buff.clear();
            StreamBlock.writeHeader(buff, stream, fromVersion, totalEntries, totalBytes, BlockCodec.NONE);
            Buffers.offsetPosition(buff, StreamBlock.HEADER_BYTES);
            Channels.writeFully(sink, buff.flip());

            buff.clear();

            long copied = 0;
            for (int idx = startIdx; idx < totalEntries; idx++) {
                var entry = entries.get(idx);
                if (entry.size > buff.remaining()) {
                    //flush to sink
                    copied += Channels.writeFully(sink, buff.flip());
                    buff.clear();
                }
                Buffers.copy(data, entry.offset, entry.size, buff);
            }

            if (buff.position() > 0) {
                copied += Channels.writeFully(sink, buff.flip());
            }

            assert totalBytes == copied;
            sink.flush();

            return totalBytes;
        }
    }

    private static final class EventEntry {
        private int offset;
        private int size;
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
            return data.slice(entry.offset, entry.size);
        }
    }

}


