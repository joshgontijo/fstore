package io.joshworks.eventry.network.tcp.internal;

public class RuntimeTimeoutException extends RuntimeException {

    public RuntimeTimeoutException(long millis) {
        super("Request timed out after" + millis + "ms");
    }
}
