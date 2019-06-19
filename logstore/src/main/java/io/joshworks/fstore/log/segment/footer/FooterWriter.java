package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.record.IDataStream;

public class FooterWriter {

    private final Storage storage;
    private final IDataStream stream;
    private final long start;

    public FooterWriter(Storage storage, IDataStream stream) {
        this.storage = storage;
        this.start = storage.position();
        this.stream = stream;
    }

    public <T> long write(T data, Serializer<T> serializer) {
        return stream.write(storage, data, serializer);
    }

    public long position() {
        return storage.position();
    }

    public void position(long position) {
        if (position < start) {
            throw new IllegalArgumentException("Position cannot be less than " + start);
        }
        storage.position(position);
    }

    public long start() {
        return start;
    }
}
