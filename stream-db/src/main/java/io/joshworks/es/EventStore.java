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

    public EventStore(File root, int logSize, int indexEntries, double bfFP, int blockSize) {
        this.log = new Log(root, logSize);
        this.index = new Index(root, indexEntries, bfFP, blockSize);
    }

    public int version(long stream) {
        IndexEntry ie = index.find(IndexKey.maxOf(stream), IndexFunction.FLOOR);
        if (ie == null || ie.stream() != stream) {
            return -1;
        }
        return ie.version();
    }

    public synchronized void linkTo(long srcStream, int srcVersion, long dstStream, int expectedVersion) {
        IndexEntry ie = index.find(new IndexKey(srcStream, srcVersion), IndexFunction.EQUALS);
        if (ie == null) {
            throw new IllegalArgumentException("No such event " + IndexKey.toString(srcStream, srcVersion));
        }
        int dstVersion = version(dstStream);
        checkVersion(expectedVersion, dstVersion);

        long seq = sequence.getAndIncrement();
        int version = dstVersion + 1;

        String data = IndexKey.toString(srcStream, srcVersion);
        ByteBuffer linkToData = Event.create(seq, dstStream, version, Buffers.wrap(data), 1); //TODO add linkto attribute

        log.append(linkToData);
        index.append(dstStream, dstVersion + 1, ie.size(), ie.logAddress());

    }

    public synchronized void append(long stream, int expectedVersion, ByteBuffer data) {
        int streamVersion = version(stream);
        checkVersion(expectedVersion, streamVersion);
        long seq = sequence.getAndIncrement();
        int version = streamVersion + 1;
        ByteBuffer eventData = Event.create(seq, stream, version, data);
        int eventSize = Event.sizeOf(eventData);
        long logPos = log.append(eventData);
        index.append(stream, version, eventSize, logPos);
    }

    private void checkVersion(int expectedVersion, int streamVersion) {
        if (expectedVersion >= 0 && expectedVersion != streamVersion) {
            throw new IllegalStateException("Version mismatch, expected " + expectedVersion + " got: " + streamVersion);
        }
    }

    public int get(long stream, int version, ByteBuffer dst) {
        IndexEntry ie = index.find(new IndexKey(stream, version), IndexFunction.EQUALS);
        if (ie == null) {
            return 0;
        }

        if (dst.remaining() < ie.size()) {
            throw new IllegalArgumentException("Not enough destination buffer space");
        }

        int plim = dst.limit();
        Buffers.offsetLimit(dst, ie.size());
        int read = log.read(ie.logAddress(), dst);
        assert ie.size() == read;
        dst.limit(plim);



        int evOffset = dst.position() - ie.size();
        Event.rewrite(dst, evOffset, stream, version);

        return read;
    }


}
