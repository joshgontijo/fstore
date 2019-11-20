package io.joshworks.fstore.tcp.internal;

public class RuntimeTimeoutException extends RuntimeException {

    public RuntimeTimeoutException(long millis) {
        super("Request timed out after " + millis + "ms");
    }
}
