package io.joshworks.es2;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

class WriteTask extends CompletableFuture<Integer> {
    final ByteBuffer event;
    int version;

    WriteTask(ByteBuffer event) {
        this.event = event;
    }
}
