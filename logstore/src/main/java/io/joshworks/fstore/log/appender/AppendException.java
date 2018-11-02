package io.joshworks.fstore.log.appender;

public class AppendException extends RuntimeException {
    public AppendException(Throwable error) {
        super(error);
    }

    public AppendException(String message) {
        super(message);
    }
}
