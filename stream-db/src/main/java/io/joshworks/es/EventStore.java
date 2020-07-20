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

    public EventStore(File root, int logSize, int indexEntries, int midpointFactor) {
        this.log = new Log(root, logSize);
        this.index = new Index(root, indexEntries, midpointFactor);
    }

    public int version(long stream) {
        IndexEntry ie = index.find(IndexKey.maxOf(stream), IndexFunction.FLOOR);
        if (ie == null) {
            return -1;
        }
        return ie.version();
    }

    public void linkTo(long srcStream, int srcVersion, long dstStream, int expectedVersion) {
        IndexEntry ie = index.find(new IndexKey(srcStream, srcVersion), IndexFunction.EQUALS);
        if (ie == null) {
            throw new IllegalArgumentException("No such event " + Event.toString(srcStream, srcVersion));
        }

    }

    public void append(long stream, int expectedVersion, ByteBuffer data) {
        int streamVersion = version(stream);
        if (expectedVersion >= 0 && expectedVersion != streamVersion) {
            throw new IllegalStateException("Version mismatch, expected " + expectedVersion + " got: " + streamVersion);
        }
        long seq = sequence.get();
        int version = streamVersion + 1;
        ByteBuffer eventData = Event.create(seq, stream, version, data);
        int eventSize = Event.sizeOf(eventData);
        long logPos = log.append(eventData);
        index.append(stream, version, eventSize, logPos);
        sequence.addAndGet(1);
    }

    public int get(long stream, int version, ByteBuffer dst) {
        IndexEntry ie = index.find(new IndexKey(stream, version), IndexFunction.EQUALS);
        if (ie == null) {
            return 0;
        }

        if (dst.remaining() < ie.size()) {
            throw new IllegalArgumentException("Not enough buffer space");
        }

        int plim = dst.limit();
        Buffers.offsetLimit(dst, ie.size());
        int read = log.read(ie.logAddress(), dst);
        assert ie.size() == read;
        dst.limit(plim);
        return read;
    }


}
