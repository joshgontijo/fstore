package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.record.IDataStream;

import java.nio.ByteBuffer;

public class FooterWriter {

    private final Storage storage;
    private final IDataStream stream;
    private final long start;

    public FooterWriter(Storage storage, IDataStream stream) {
        this.storage = storage;
        this.start = storage.position();
        this.stream = stream;
    }

    public long write(ByteBuffer data) {
        return stream.write(storage, data);
    }

    public long write(ByteBuffer[] data) {
        return stream.write(storage, data);
    }

    public long position() {
        return storage.position();
    }

    public long skip(long bytes) {
        long newPos = position() + bytes;
        position(newPos);
        return newPos;
    }

    public void position(long position) {
        if (position < start) {
            throw new IllegalArgumentException("Position cannot be less than " + start);
        }
        storage.position(position);
    }

    public long length() {
        return position() - start;
    }

    public long start() {
        return start;
    }
}
