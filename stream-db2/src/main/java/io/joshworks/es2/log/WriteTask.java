package io.joshworks.es2.log;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

class WriteTask extends CompletableFuture<Long> {
    final ByteBuffer data;
    final long sequence;
    final Type type;

    WriteTask(ByteBuffer data, long sequence, Type type) {
        this.data = data;
        this.sequence = sequence;
        this.type = type;
    }
}
