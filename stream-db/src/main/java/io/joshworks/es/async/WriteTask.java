package io.joshworks.es.async;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class WriteTask extends CompletableFuture<Void> {

    final Consumer<StoreWriter> handler;

    public WriteTask(Consumer<StoreWriter> handler) {
        this.handler = handler;
    }
}
