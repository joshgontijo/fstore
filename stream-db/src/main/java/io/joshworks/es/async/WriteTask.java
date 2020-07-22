package io.joshworks.es.async;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class WriteTask extends CompletableFuture<TaskResult> {

    final ByteBuffer data;

    TaskResult result;


    public WriteTask(ByteBuffer data) {
        this.data = data;
    }
}
