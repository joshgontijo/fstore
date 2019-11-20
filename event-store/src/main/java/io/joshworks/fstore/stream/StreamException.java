package io.joshworks.fstore.stream;

public class StreamException extends RuntimeException {

    public StreamException() {
    }

    public StreamException(String message) {
        super(message);
    }

    public StreamException(String message, Throwable cause) {
        super(message, cause);
    }

    public StreamException(Throwable cause) {
        super(cause);
    }

    public StreamException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
