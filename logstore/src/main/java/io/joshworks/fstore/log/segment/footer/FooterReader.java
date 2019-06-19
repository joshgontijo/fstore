package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.record.RecordEntry;

public class FooterReader {

    private final Storage storage;
    private final IDataStream stream;
    private final long startPos;
    private final long len;
    private long currPos;

    public FooterReader(Storage storage, IDataStream stream, long startPos, long len) {
        this.storage = storage;
        this.stream = stream;
        this.startPos = startPos;
        this.len = len;
        this.currPos = startPos;
    }

    public <T> T read(Serializer<T> serializer) {
        long maxPos = startPos + len;
        if (currPos < startPos || currPos >= maxPos) {
            return null;
        }
        RecordEntry<T> entry = stream.read(storage, Direction.FORWARD, currPos, serializer);
        if (entry == null) {
            return null;
        }
        currPos += currPos + entry.recordSize();
        return entry.entry();
    }

    public long start() {
        return startPos;
    }

    public long length() {
        return len;
    }

    public long position() {
        return storage.position();
    }

    public void position(long position) {
        long maxPos = startPos + len;
        if (position < startPos || position > maxPos) {
            throw new IllegalStateException("Invalid footer position: " + position + ", allowed range is: " + startPos + " - " + maxPos);
        }
        this.currPos = position;
    }

}
