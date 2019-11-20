package io.joshworks.fstore.server.cluster;

public class RemoteClientException extends RuntimeException {

    public RemoteClientException() {
    }

    public RemoteClientException(String message) {
        super(message);
    }

    public RemoteClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public RemoteClientException(Throwable cause) {
        super(cause);
    }

    public RemoteClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
