package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.record.RecordEntry;

public class FooterReader {

    private final Storage storage;
    private final IDataStream stream;
    private final long start;
    private final long length;

    private long currPos;

    public FooterReader(Storage storage, IDataStream stream, long start, long len) {
        this.storage = storage;
        this.stream = stream;
        this.start = start;
        this.length = len;
        this.currPos = start;
    }

    public <T> T read(Serializer<T> serializer) {
        RecordEntry<T> entry = readInternal(currPos, serializer);
        if (entry == null) {
            return null;
        }
        currPos += currPos + entry.recordSize();
        return entry.entry();
    }

    public <T> T read(long pos, Serializer<T> serializer) {
        RecordEntry<T> entry = readInternal(pos, serializer);
        if (entry == null) {
            return null;
        }
        return entry.entry();
    }

    private <T> RecordEntry<T> readInternal(long pos, Serializer<T> serializer) {
        if (!withinBounds(pos)) {
            return null;
        }
        return stream.read(storage, Direction.FORWARD, pos, serializer);
    }


    public long start() {
        return start;
    }

    public long length() {
        return length;
    }

    public long position() {
        return storage.position();
    }

    public void position(long position) {
        if (!withinBounds(position)) {
            throw new IllegalStateException("Invalid footer position: " + position + ", allowed range is: " + start + " - " + maxPos());
        }
        this.currPos = position;
    }

    private boolean withinBounds(long position) {
        return position >= start && position <= maxPos();
    }

    private long maxPos() {
        return start + length - 1;
    }

}
