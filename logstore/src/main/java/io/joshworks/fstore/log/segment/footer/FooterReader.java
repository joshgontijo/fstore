package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.SegmentException;

import java.nio.ByteBuffer;

public class FooterReader {

    private final Storage storage;
    private final long startPos;
    private final long len;

    public FooterReader(Storage storage, long startPos, long len) {
        this.storage = storage;
        this.startPos = startPos;
        this.len = len;
    }

    public int read(long pos, ByteBuffer data) {
        long maxPos = startPos + len;
        if (pos < startPos || pos >= maxPos) {
            throw new SegmentException("Footer position must be between " + startPos + " and " + maxPos + ", got " + pos);
        }
        return storage.read(pos, data);
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
