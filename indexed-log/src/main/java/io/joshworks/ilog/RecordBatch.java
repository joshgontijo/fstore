package io.joshworks.ilog;

import java.nio.ByteBuffer;

public class RecordBatch {

    private final ByteBuffer chunk;

    public RecordBatch(ByteBuffer chunk) {
        this.chunk = chunk;
    }
}
