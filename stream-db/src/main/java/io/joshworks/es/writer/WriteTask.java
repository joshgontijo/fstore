package io.joshworks.es.writer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class WriteTask extends CompletableFuture<Void> {

    final Consumer<StoreWriter> handler;

    public WriteTask(Consumer<StoreWriter> handler) {
        this.handler = handler;
    }
}
