package io.joshworks.es;

import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexFunction;
import io.joshworks.es.index.IndexSegment;
import io.joshworks.es.log.Log;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class EventStore {

    private final Index index = new Index();
    private final Log log = new Log();

    private final AtomicLong sequence = new AtomicLong();

    public int version(long stream) {
        long address = index.find(stream, Integer.MAX_VALUE, IndexFunction.FLOOR);
        long foundStream = index.stream(address);
        if (stream != foundStream) {
            return -1;
        }
        return index.version(address);
    }

    public void append(long stream, int expectedVersion, ByteBuffer data) {
        int streamVersion = version(stream);
        if (expectedVersion >= 0 && expectedVersion != streamVersion) {
            throw new IllegalStateException("Version mismatch");
        }
        long seq = sequence.get() + 1;
        int version = streamVersion + 1;
        ByteBuffer eventData = Event.create(seq, stream, version, data);
        int eventSize = eventData.remaining();
        long logPos = log.append(eventData);
        index.append(stream, version, eventSize, logPos);
        sequence.addAndGet(1);
    }

    public Event get(long stream, int version) {
        long address = index.find(stream, version, IndexFunction.EQUALS);
        if (address == IndexSegment.NONE) {
            return null;
        }
        int size = index.size(address);
        long logAddress = index.logAddress(address);

        ByteBuffer buffer = null;

        log.read(buffer, logAddress);
        return new Event();
    }


}
