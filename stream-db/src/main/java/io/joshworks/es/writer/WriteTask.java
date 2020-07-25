package io.joshworks.es.writer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class WriteTask extends CompletableFuture<Void> {

    final Consumer<BatchingWriter> handler;

    WriteTask(Consumer<BatchingWriter> handler) {
        this.handler = handler;
    }
}
