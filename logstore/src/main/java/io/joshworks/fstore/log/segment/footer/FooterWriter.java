package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.io.Storage;

import java.nio.ByteBuffer;

public class FooterWriter {

    private final Storage storage;

    public FooterWriter(Storage storage) {
        this.storage = storage;
    }

    public long write(ByteBuffer data) {
        return storage.write(data);
    }

    public long position() {
        return storage.writePosition();
    }

}
