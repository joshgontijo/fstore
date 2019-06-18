package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.record.IDataStream;

public class FooterWriter {

    private final Storage storage;
    private final IDataStream stream;
    private final long start;

    public FooterWriter(Storage storage) {
        this.storage = storage;
    }

    public <T> long write(T data, Serializer<T> serializer) {
        return stream.write(storage, data, serializer);
    }

    public long position() {
        return storage.writePosition();
    }

    //TODO test mem storage for the following scenario:
    //Set writePosition greater than store size
    //write data
    //buffer should expand to the position when a write is performed
    //reads without writes should return EOF
    public void position(long position) {
        if (position < start) {
            throw new IllegalArgumentException("Position cannot be less than " + start);
        }
        storage.writePosition(position);
    }

    public long start() {
        return start;
    }
}
