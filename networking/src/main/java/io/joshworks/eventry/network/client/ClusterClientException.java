package io.joshworks.eventry.network.client;

public class ClusterClientException extends RuntimeException {

    public ClusterClientException() {
    }

    public ClusterClientException(String message) {
        super(message);
    }

    public ClusterClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClusterClientException(Throwable cause) {
        super(cause);
    }

    public ClusterClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
