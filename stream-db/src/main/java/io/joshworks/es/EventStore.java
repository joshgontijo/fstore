package io.joshworks.es;

import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexFunction;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class EventStore {

    private final Log log;
    private final Index index;

    private final AtomicLong sequence = new AtomicLong();

    public EventStore(File root, int logSize, int indexEntries) {
        this.log = new Log(new File(root, "log"), logSize);
        this.index = new Index(new File(root, "index"), indexEntries);
    }

    public int version(long stream) {
        IndexEntry ie = index.find(IndexKey.maxOf(stream), IndexFunction.FLOOR);
        if (ie == null) {
            return -1;
        }
        return ie.version;
    }

    public void append(long stream, int expectedVersion, ByteBuffer data) {
        int streamVersion = version(stream);
        if (expectedVersion >= 0 && expectedVersion != streamVersion) {
            throw new IllegalStateException("Version mismatch");
        }
        long seq = sequence.get();
        int version = streamVersion + 1;
        ByteBuffer eventData = Event.create(seq, stream, version, data);
        int eventSize = Event.sizeOf(eventData);
        long logPos = log.append(eventData);
        index.append(stream, version, eventSize, logPos);
        sequence.addAndGet(1);
    }

    public Event get(long stream, int version) {
        IndexEntry ie = index.find(new IndexKey(stream, version), IndexFunction.EQUALS);
        if (ie == null) {
            return null;
        }
        ByteBuffer buffer = Buffers.allocate(ie.size, false);

        log.read(buffer, ie.logAddress);
        return new Event();
    }


}
