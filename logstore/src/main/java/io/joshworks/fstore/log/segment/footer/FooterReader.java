package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.SegmentException;

import java.nio.ByteBuffer;

public class FooterReader {

    private final Storage storage;
    private final IDataStream stream;
    private final long startPos;
    private final long len;

    public FooterReader(Storage storage, long startPos, long len) {
        this.storage = storage;
        this.startPos = startPos;
        this.len = len;
    }

    public <T> RecordEntry<T> read(long pos, Serializer<T> serializer) {
        long maxPos = startPos + len;
        if (pos < startPos || pos >= maxPos) {
            throw new SegmentException("Footer position must be between " + startPos + " and " + maxPos + ", got " + pos);
        }
        return stream.read(storage, Direction.FORWARD, pos, serializer);
    }

    public long start() {
        return startPos;
    }

    public long length() {
        return len;
    }

    public long position() {
        return storage.writePosition();
    }

}
