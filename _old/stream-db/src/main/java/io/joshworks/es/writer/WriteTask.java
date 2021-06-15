package io.joshworks.es.writer;

import java.util.concurrent.CompletableFuture;

public class WriteTask extends CompletableFuture<Void> {

    final Runnable handler;

    WriteTask(Runnable handler) {
        this.handler = handler;
    }
}
